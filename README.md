# swim

This is a Haskell implementation of the [SWIM epidemic gossip protocol](https://www.cs.cornell.edu/~asdas/research/dsn02-swim.pdf): Scalable Weakly-consistent Infection-style Process Group Membership Protocol.

It's developed to be compliant with Hashicorp's excellent implementation of the protocol https://github.com/hashicorp/memberlist (which backs [Serf](https://www.serfdom.io/docs/internals/gossip.html) and used by [Consul](https://www.consul.io/)). That means it conforms to their wire protocol and will need to support some additional features they introduce.

Overall, I'm aiming for basic functionality rather than performance or robust failure handling. This is my first Haskell project so I want to set my expectations accordingly :)

[![Build Status](https://travis-ci.org/jpfuentes2/swim.svg?branch=master)](https://travis-ci.org/jpfuentes2/swim)
