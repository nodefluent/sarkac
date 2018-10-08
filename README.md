<h1 align="center">sarkac</h1>
<p align="center">
  <img alt="sarkac" src="553px-Empirical_rule_histogram.svg.png" width="362">
</p>
<p align="center">
  Apache Kafka topic and message anomaly detection with automated discovery.
</p>

[![npm version](https://badge.fury.io/js/sarkac.svg)](https://badge.fury.io/js/sarkac)

## Install

Very simple via `yarn add sarkac`s

## Setup

Basically you can create your own apps with sarkac or integrate it in your existing apps,
however you can also simply just spin up an instance by providing it some simple configuration info.

You can find an example [here](example/example.js).

Visit `http://localhost:8033/` to check for sarkac's HTTP endpoints, that give infos about discovery and
anomaly stats.

## How does it work?

sarkac connects to your Kafka cluster and runs a simple discovery protocol to detect existing Kafka topics,
it will automatically subscribe to them (also to newly added Kafka topics) and analyse their schema (has to be JSON),
of their message payloads. It will then identify any fields of type 'number' and track them across all messages it
receives. sarkac then uses MongoDB to keep multiple (as much as you configure) rolling windows of the values of
the tracked fields. It runs the [68–95–99.7 rule](https://en.wikipedia.org/wiki/68%E2%80%9395%E2%80%9399.7_rule)
on every window of every tracked field continously to detect anomalies.
If an anomaly is detected it produces its information to an anomaly Kafka topic.

## Running without auto-discovery

As shown in the example (uncommented dsl lines) it is also possible to deactivate auto discovery of topics and fields
and simply run sarkac on fixed topics, by configuring the `config.dsl` object, do not forget to turn off discovery via
`config.discovery.enabled = false`.

Additionally you can also turn off anomaly production to Kafka via `config.target.produceAnomalies = false`.

## Use-case

Given a Kafka cluster with a certain amount of topics, keeping an eye on all of them at once can be challenging.
And although we do not claim that you can cover all kinds of anomalies with sarkac, it can at least help you to
tackle certain problems earlier. Just spin up a few instances and let them disover you Kafka broker's topics and produce
anomalies to an output topic. Use our [Kafka to Prometheus Connector](https://github.com/nodefluent/prometheus-kafka-connect)
to sink the anomaly topic into Prometheus and use Grafanas alert magic to get you notified based on detected anomalies.

## Maintainer

Build with :heart: :pizza: and :coffee: by [nodefluent](https://github.com/nodefluent)