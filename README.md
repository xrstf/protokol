# protokol - Collect Kubernetes Pod Logs with Ease

`protokol` is a command line tool to watch for pods and storing their logs as text files
on your local machine. It's primarily intended to be used in test scenarios where you
need to gather logs of many different, possibly restarting or reconciled pods at the
same time.

<p align="center">
  <img src="docs/sample.png" />
</p>

## Installation

You can download a binary for the [latest release on GitHub](https://github.com/xrstf/protokol/releases)
or install protokol via Go:

```bash
go install go.xrstf.de/protokol
```

## Usage

```
Usage of protokol:
  -c, --container stringArray   Container names to store logs for (supports glob expression) (can be given multiple times)
  -f, --flat                    Do not create directory per namespace, but put all logs in the same directory
      --kubeconfig string       kubeconfig file to use (uses $KUBECONFIG by default)
  -l, --labels string           Label-selector as an alternative to specifying resource names
      --live                    Only consider running pods, ignore completed/failed pods
  -n, --namespace stringArray   Kubernetes namespace to watch resources in (supports glob expression) (can be given multiple times)
      --oneshot                 Dump logs, but do not tail the containers (i.e. exit after downloading the current state)
  -o, --output string           Directory where logs should be stored
  -v, --verbose                 Enable more verbose output
```

## Examples

```bash
protokol '*'
```

This is the BFG: it will watch all Pods in all namespaces and stream the logs for each running container to
a text file in your disk. Don't do this, you do not want to kill the apiserver with a gazillion streams.

```bash
protokol -n kube-system -n 'cluster-*'
```

You can restrict the Pods to a set of namespaces. Wildcards are allowed.

```bash
protokol -l 'foo=bar'
```

Label selectors work just as you would expect.

```bash
protokol 'kube-*' 'coredns-*' 'etcd-*'
```

You can give multiple name patterns, placeholders are allowed.

```bash
protokol -c 'test*' 'kube-*' 'coredns-*' 'etcd-*'
```

You can restrict the container names using `-c`. This flag can be given multiple times and also supports wildcards.

```bash
protokol -o test 'kube-*' 'coredns-*' 'etcd-*'
```

By default protokol will create a `protokol-%Y%m%dT%m.%h%s` directory whenever you invoke it, but you can also specify
a directory yourself.

```bash
protokol -o test -f 'kube-*' 'coredns-*' 'etcd-*'
```

By default protokol will create one directory per namespace. With `-f` (`--flat`) you can disable this behaviour.

## License

MIT
