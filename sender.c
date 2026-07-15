/* sender.c — NACK-based ARQ sender.
 *
 * Strategy:
 *   - Forward every frame to the relay immediately (same as baseline).
 *   - Keep the last BUF_SIZE frames in a circular buffer.
 *   - Listen for 4-byte NACK packets on port 47004 (relayed from receiver).
 *   - On NACK: look up the missing frame and resend it immediately.
 *   - Use select() to multiplex between new harness frames and incoming NACKs.
 *
 * Wire format (forward, 164 bytes = 1.025x overhead):
 *   [0..3]   seq      (4 bytes, big-endian uint32)
 *   [4..163] payload  (160 bytes, verbatim from harness)
 *
 * NACK format (incoming from port 47004, 4 bytes):
 *   [0..3]   missing_seq  (4 bytes, big-endian uint32)
 *
 * Ports:
 *   bind 47010  <- harness source delivers frames here
 *   send 47001  -> relay uplink toward receiver
 *   bind 47004  <- NACK feedback from receiver, via relay
 *
 * build: gcc -O2 -o sender sender.c
 */
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdint.h>

#define FRAME_LEN  164    /* 4-byte seq + 160-byte payload                */
#define BUF_SIZE   128    /* circular retransmit buffer (power-of-2)      */
#define BUF_MASK   (BUF_SIZE - 1)

/* One slot in the retransmit circular buffer.
 * We store the full 164-byte frame ready for immediate re-send.          */
typedef struct {
    uint32_t      seq;
    unsigned char pkt[FRAME_LEN];
    int           valid;
} Frame;

static Frame sent_buf[BUF_SIZE];

static inline uint32_t read_seq(const unsigned char *b) {
    return ((uint32_t)b[0] << 24) | ((uint32_t)b[1] << 16) |
           ((uint32_t)b[2] <<  8) |  (uint32_t)b[3];
}

int main(void) {
    /* ── Socket: receive new frames from harness on 47010 ─────────────── */
    int in_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (in_fd < 0) { perror("socket in_fd"); return 1; }

    struct sockaddr_in in_addr = {0};
    in_addr.sin_family      = AF_INET;
    in_addr.sin_port        = htons(47010);
    in_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (bind(in_fd, (struct sockaddr *)&in_addr, sizeof in_addr) < 0) {
        perror("bind 47010"); return 1;
    }

    /* ── Socket: forward frames to relay on 47001 ─────────────────────── */
    int out_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (out_fd < 0) { perror("socket out_fd"); return 1; }

    struct sockaddr_in relay = {0};
    relay.sin_family      = AF_INET;
    relay.sin_port        = htons(47001);
    relay.sin_addr.s_addr = inet_addr("127.0.0.1");

    /* ── Socket: receive NACK messages from relay on 47004 ────────────── */
    int nack_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (nack_fd < 0) { perror("socket nack_fd"); return 1; }

    struct sockaddr_in nack_addr = {0};
    nack_addr.sin_family      = AF_INET;
    nack_addr.sin_port        = htons(47004);
    nack_addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    if (bind(nack_fd, (struct sockaddr *)&nack_addr, sizeof nack_addr) < 0) {
        perror("bind 47004"); return 1;
    }

    int maxfd = (in_fd > nack_fd ? in_fd : nack_fd) + 1;
    unsigned char buf[2048];

    /* ── Main event loop ──────────────────────────────────────────────── */
    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(in_fd,   &rfds);
        FD_SET(nack_fd, &rfds);

        /* Block until either a new frame or a NACK is ready */
        if (select(maxfd, &rfds, NULL, NULL, NULL) < 0) continue;

        /* ── Priority 1: handle incoming NACKs (retransmit immediately) ─ */
        if (FD_ISSET(nack_fd, &rfds)) {
            ssize_t n = recvfrom(nack_fd, buf, sizeof buf, 0, NULL, NULL);
            if (n >= 4) {
                uint32_t missing = read_seq(buf);
                Frame *f = &sent_buf[missing & BUF_MASK];
                if (f->valid && f->seq == missing) {
                    /* Resend the stored frame verbatim */
                    sendto(out_fd, f->pkt, FRAME_LEN, 0,
                           (struct sockaddr *)&relay, sizeof relay);
                }
                /* If the frame has aged out of the buffer (> BUF_SIZE frames
                 * ago), we can no longer recover it; the receiver must skip. */
            }
        }

        /* ── Priority 2: forward new frame from harness ─────────────────
         * Done after NACKs to ensure retransmissions get priority over
         * new traffic when both arrive simultaneously.                   */
        if (FD_ISSET(in_fd, &rfds)) {
            ssize_t n = recvfrom(in_fd, buf, sizeof buf, 0, NULL, NULL);
            if (n != FRAME_LEN) continue;  /* sanity: harness always sends 164 */

            uint32_t seq = read_seq(buf);

            /* Store in circular buffer for potential retransmission */
            Frame *f = &sent_buf[seq & BUF_MASK];
            f->seq   = seq;
            f->valid = 1;
            memcpy(f->pkt, buf, FRAME_LEN);

            /* Forward to relay immediately */
            sendto(out_fd, buf, FRAME_LEN, 0,
                   (struct sockaddr *)&relay, sizeof relay);
        }
    }
    return 0;
}

