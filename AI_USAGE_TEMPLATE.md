# AI / Coding-Assistant Usage

## Tools used
List the generative-AI/coding tools used by the team and what each was used for.

- GPT-6.1-Sol medium (Codex) For quick-testing, development and troubleshooting of the flow control

## One useful suggestion
Describe one suggestion that materially helped the project and how you verified it.

We discovered that our flow control initially had issues because even with 1 supposedly normal client, after a while, our client would eventually move to the FINALS_ONLY state, despite no evident reason for traffic to be throttled. Our flow control attempted to measure and compare the throughput of the source->server and server->client. By utilizing AI agents from the Codex harness, we had the reality check that our FINAL_ONLY state, where draft segments are purposely held off and only finals sent, would interfere with how we measure throughput (less segments are sent at a given time). There were also other realizations, which are recorded in the report. Our LLM then wisely suggested that we should utilize a combination of more reliable metrics, such "delivery pressure" or how long has a final segment been sittting in queue for. We verified the effcetivness both by testing the application ourselves, as well as analyzing the code written by the AI and "interrogating" it by asking the agent why certain parts of the code were required. This resulted in a more reliable system.


## One suggestion that was wrong, inefficient, or required modification

Include a short generated suggestion/code fragment or precise description, explain the problem, and describe your correction.

## Responsibility statement

We understand that the team remains responsible for the submitted code, experimental results, and claims, and that every member may be asked to explain or modify relevant code during the viva.
