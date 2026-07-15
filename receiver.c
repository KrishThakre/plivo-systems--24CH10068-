/* receiver.c — NACK-based ARQ receiver.
 *
 * Strategy:
 *   - Receive forward packets (164 bytes each) from the relay.
 *   - Store them in a ring buffer keyed by seq.
 *   - Track the highest received seq. Any gap below it is a candidate loss.
 *   - Send a 4-byte NACK immediately when a gap is detected; re-NACK after
 *     RENACK_MS if the frame still hasn't arrived (handles NACK loss).
 *   - Deliver frames to the player in strict seq order.
 *   - When T0 + DELAY_MS env vars are available, use wall-clock deadlines to
 *     flush unrecoverable gaps (avoid stalling behind a permanent loss).
 *
 * Wire format (incoming, 164 bytes):
 *   [0..3]   seq      (4 bytes, big-endian uint32)
 *   [4..163] payload  (160 bytes audio data)
 *
 * NACK format (outgoing to port 47003, 4 bytes):
 *   [0..3]   missing_seq  (4 bytes, big-endian uint32)
 *
 * Ports:
 *   bind 47002  <- media from sender, via hostile relay
 *   send 47020  -> harness player (4-byte seq + 160-byte payload)
 *   send 47003  -> NACK feedback toward sender, via relay
 *
 * build: gcc -O2 -o receiver receiver.c
 */
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#define PAYLOAD_LEN  160
#define FRAME_LEN    164    /* output to player: 4-byte seq + 160-byte payload */
#define BUF_SIZE     128    /* ring buffer slots (power-of-2)                  */
#define BUF_MASK     (BUF_SIZE - 1)

/* How long to wait before re-NACKing a still-missing frame.
 * Set to ~1 RTT so a lost NACK gets retried before the deadline.         */
#define RENACK_MS    25     /* milliseconds */

/* ── Ring buffer ──────────────────────────────────────────────────────── */
typedef struct {
    uint32_t      seq;
    unsigned char payload[PAYLOAD_LEN];
    int           valid;
} Slot;

static Slot ring[BUF_SIZE];

static void ring_store(uint32_t seq, const unsigned char *payload) {
    Slot *s = &ring[seq & BUF_MASK];
    if (!s->valid || s->seq != seq) {
        s->seq   = seq;
        s->valid = 1;
        memcpy(s->payload, payload, PAYLOAD_LEN);
    }
}

/* Returns 1 and copies payload if present; returns 0 on miss.
 * Pass out=NULL to probe without copying.                                */
static int ring_fetch(uint32_t seq, unsigned char *out) {
    const Slot *s = &ring[seq & BUF_MASK];
    if (s->valid && s->seq == seq) {
        if (out) memcpy(out, s->payload, PAYLOAD_LEN);
        return 1;
    }
    return 0;
}

static void ring_clear(uint32_t seq) {
    Slot *s = &ring[seq & BUF_MASK];
    if (s->valid && s->seq == seq) s->valid = 0;
}

/* ── NACK tracking ────────────────────────────────────────────────────── */
/* Per-slot: the seq that was last NACKed, and when (seconds since epoch).
 * Allows re-NACKing after RENACK_MS if the frame still hasn't arrived.  */
static uint32_t nack_seq[BUF_SIZE];
static double   nack_time[BUF_SIZE];

static inline double now_s(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec * 1e-9;
}

static void send_nack(int fd, struct sockaddr_in *dst,
                      uint32_t seq, double now) {
    if (ring_fetch(seq, NULL)) return;  /* frame already received */

    uint32_t idx = seq & BUF_MASK;
    if (nack_seq[idx] == seq) {
        /* Already NACKed this seq: re-NACK only after the cooldown */
        if (now - nack_time[idx] < RENACK_MS * 1e-3) return;
    }

    nack_seq[idx]  = seq;
    nack_time[idx] = now;

    unsigned char pkt[4] = {
        (seq >> 24) & 0xFF,
        (seq >> 16) & 0xFF,
        (seq >>  8) & 0xFF,
         seq        & 0xFF
    };
    sendto(fd, pkt, 4, 0, (struct sockaddr *)dst, sizeof *dst);
}

/* ── Delivery helpers ─────────────────────────────────────────────────── */
static inline void deliver(int out_fd, struct sockaddr_in *player,
                           uint32_t seq, const unsigned char *payload) {
    unsigned char pkt[FRAME_LEN];
    pkt[0] = (seq >> 24) & 0xFF;
    pkt[1] = (seq >> 16) & 0xFF;
    pkt[2] = (seq >>  8) & 0xFF;
    pkt[3] =  seq        & 0xFF;
    memcpy(pkt + 4, payload, PAYLOAD_LEN);
    sendto(out_fd, pkt, FRAME_LEN, 0,
           (struct sockaddr *)player, sizeof *player);
}

/* Deliver all consecutive in-order frames starting at *next_seq.
 * Advances *next_seq past every delivered frame.                         */
static void flush_inorder(int out_fd, struct sockaddr_in *player,
                          uint32_t *next_seq) {
    unsigned char payload[PAYLOAD_LEN];
    while (ring_fetch(*next_seq, payload)) {
        deliver(out_fd, player, *next_seq, payload);
        ring_clear(*next_seq);
        (*next_seq)++;
    }
}

int main(void) {
    /* ── Timing parameters (for deadline-based gap flushing) ─────────── */
    const char *t0_env    = getenv("T0");
    const char *delay_env = getenv("DELAY_MS");
    double T0      = t0_env    ? atof(t0_env)    : 0.0;
    double delay_s = delay_env ? atof(delay_env) / 1000.0 : 0.040;
    int use_timing = (T0 > 1.0);  /* guard against unset / zero T0 */

    /* ── Socket: receive forward packets from relay on 47002 ─────────── */
    int in_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (in_fd < 0) { perror("socket in_fd"); return 1; }

    struct sockaddr_in in_addr = {0};
    in_addr.sin_family      = AF_INET;
    in_addr.sin_port        = htons(47002);
    in_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (bind(in_fd, (struct sockaddr *)&in_addr, sizeof in_addr) < 0) {
        perror("bind 47002"); return 1;
    }

    /* ── Socket: deliver frames to harness player on 47020 ───────────── */
    int out_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (out_fd < 0) { perror("socket out_fd"); return 1; }

    struct sockaddr_in player = {0};
    player.sin_family      = AF_INET;
    player.sin_port        = htons(47020);
    player.sin_addr.s_addr = inet_addr("127.0.0.1");

    /* ── Socket: send NACKs to relay (which forwards to sender:47004) ── */
    int nack_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (nack_fd < 0) { perror("socket nack_fd"); return 1; }

    struct sockaddr_in nack_dst = {0};
    nack_dst.sin_family      = AF_INET;
    nack_dst.sin_port        = htons(47003);
    nack_dst.sin_addr.s_addr = inet_addr("127.0.0.1");

    /* ── State ───────────────────────────────────────────────────────── */
    uint32_t next_seq   = 0;  /* next frame we need to deliver in order   */
    uint32_t highest_rx = 0;  /* highest seq received so far              */
    int      started    = 0;

    unsigned char in_buf[2048];

    /* ── Main event loop ─────────────────────────────────────────────── */
    for (;;) {
        /* 5ms timeout: drives re-NACK and deadline flushing even when no
         * packets arrive (e.g., consecutive drops).                      */
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(in_fd, &rfds);
        struct timeval tv = {0, 5000};  /* 5 ms */
        int ret = select(in_fd + 1, &rfds, NULL, NULL, &tv);

        if (ret > 0 && FD_ISSET(in_fd, &rfds)) {
            /* ── Process incoming forward packet ──────────────────────── */
            ssize_t n = recvfrom(in_fd, in_buf, sizeof in_buf, 0, NULL, NULL);
            if (n != FRAME_LEN) goto periodic;  /* ignore wrong-size packets */

            uint32_t seq           = ((uint32_t)in_buf[0] << 24) |
                                     ((uint32_t)in_buf[1] << 16) |
                                     ((uint32_t)in_buf[2] <<  8) |
                                      (uint32_t)in_buf[3];
            const unsigned char *payload = in_buf + 4;

            if (!started) {
                next_seq   = seq;
                highest_rx = seq;
                started    = 1;
            }

            /* Store in ring buffer (handles duplicates gracefully) */
            ring_store(seq, payload);
            if (seq > highest_rx) highest_rx = seq;

            /* ── NACK all gaps below the newly received seq ────────────
             * Limit to BUF_SIZE window; older gaps can't be in the
             * sender's buffer anyway.                                    */
            {
                double t = now_s();
                uint32_t lo = (next_seq + BUF_SIZE < highest_rx)
                              ? highest_rx - BUF_SIZE : next_seq;
                for (uint32_t s = lo; s < highest_rx; s++) {
                    send_nack(nack_fd, &nack_dst, s, t);
                }
            }

            /* ── Deliver available in-order frames ─────────────────── */
            flush_inorder(out_fd, &player, &next_seq);
        }

    periodic:
        /* ── Periodic work (runs every ≤5ms and after every packet) ── */
        if (!started) continue;

        {
            double t = now_s();

            /* Re-NACK still-missing gaps (handles lost NACKs) */
            uint32_t lo = (next_seq + BUF_SIZE < highest_rx)
                          ? highest_rx - BUF_SIZE : next_seq;
            for (uint32_t s = lo; s < highest_rx; s++) {
                send_nack(nack_fd, &nack_dst, s, t);
            }

            /* Deadline flush: if T0 is set, skip frames whose delivery
             * deadline has expired so we don't stall behind a permanent
             * loss.
             *
             * deadline(seq) = T0 + delay_s + seq * 0.020
             * We flush when now > deadline + 1ms grace.                */
            if (use_timing) {
                while (t > T0 + delay_s + next_seq * 0.020 + 0.001) {
                    /* Try to deliver (might have arrived via retransmit) */
                    unsigned char payload[PAYLOAD_LEN];
                    if (ring_fetch(next_seq, payload)) {
                        deliver(out_fd, &player, next_seq, payload);
                    }
                    /* Advance regardless — deadline is gone */
                    ring_clear(next_seq);
                    next_seq++;
                    /* Flush any consecutive available frames */
                    flush_inorder(out_fd, &player, &next_seq);
                }
            }
        }
    }
    return 0;
}

