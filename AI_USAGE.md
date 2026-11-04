# AI / Coding-Assistant Usage

## Tools used

Codex was used during the development of the browser client in
`client/index.html` and `client/app.js`. Its use was limited to suggesting
UI structure wording and explanatory comments for the live-caption panel and
the recall panel. It also helped review the relationship between the HTML
layout and the JavaScript responsible for caption rendering scrolling and
reconnect notices.

The generated suggestions were treated as drafts. The team selected and
adapted the relevant parts to fit the existing WebSocket protocol and the
assignment requirements. No generated change was accepted without checking
its effect on the existing interface and client behaviour.

## One useful suggestion


## One suggestion that was wrong inefficient or required modification

An experimental suggestion on the `experiment/enhance-pong` branch used the
number of healthy pong responses and a short draft hold period to decide when
to resume provisional captions. This was useful as a diagnostic experiment
but it could cause draft delivery to change too quickly when network delay
varied. In particular, it performed less reliably in the T3 slow-client test
because recovery could remain delayed and a client could miss the final
caption near the end of the run. The team therefore treated it as an
experimental alternative rather than part of the final flow-control policy.
The branch was tested separately and its behaviour was compared with the
queue and finalised-caption delivery requirements before the final
implementation was selected.

## Responsibility statement

We understand that the team remains responsible for the submitted code
experimental results and claims. Every member may be asked to explain or
modify relevant code during the viva.
