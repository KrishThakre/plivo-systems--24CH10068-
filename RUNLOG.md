# Plivo Systems Assignment Run Log

## Final Valid Submission
* **Profile:** A
* **Playout Delay:** 50 ms
* **Miss Rate:** < 1.00%
* **Bandwidth Overhead:** < 2.00x
* **Result:** VALID

## Architecture Notes
To crack the 50ms delay, the round-trip time (RTT) was too high for standard ARQ (NACKs). I implemented a **Dynamic Token Bucket Forward Error Correction (FEC)** strategy. 
* The sender dynamically monitors real-time bandwidth usage. 
* While safely under the 2.00x cap, it aggressively piggybacks frames N-1 and N-2 to survive burst packet drops without requiring a second round trip.
* If the running bandwidth ratio hits 1.97x, it temporarily throttles redundancy to ensure the strict < 2.00x limit is mathematically respected.
