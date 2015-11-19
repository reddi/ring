import string
import random
import bisect
import md5


def rand_str(length=10):
    return ''.join(
        random.choice(string.letters + string.digits)
        for i in xrange(length)
    )


def keyhash(key):
    return long(md5.md5(key).hexdigest(), 16)


class ConsistentHashRing(object):

    def __init__(self, rcount=3):
        self.rcount = rcount
        self.partitions = {}
        self._hashnames = []

    def _repl_iterator(self, key):
        return (
            keyhash('{}:{}'.format(key, i)) for i in xrange(self.rcount)
        )

    @property
    def hashnames(self):
        if (self._hashnames is None or
            len(self._hashnames) != len(self.partitions)):

            self._hashnames = sorted([p for p in self.partitions])

        return self._hashnames

    def __setitem__(self, name, partition):
        hash_ = keyhash(name)
        self.partitions[hash_] = partition

    def __getitem__(self, key):
        result = []
        for hash_ in self._repl_iterator(key):
            start = bisect.bisect_right(self.hashnames, hash_)
            if start == len(self.hashnames):
                start = 0

            result.append(self.partitions[self.hashnames[start]])
        return result

    def __delitem__(self, name):
        hash_ = keyhash(name)
        del self.partitions[hash_]
        index = bisect.bisect_right(self.hashnames, hash_)
        del self._hashnames[index]


class Disk(object):

    def __init__(self, name):
        self.name = name
        self.keys = []

    def write(self, key):
        self.keys.append(key)


class Node(object):

    def __init__(self, name, disks_count=24):
        self.name = name
        self.disks = {}

        for _ in xrange(disks_count):
            dname = rand_str(10)
            self.disks[dname] = Disk(dname)

    def has_disk(self, disk):
        return disk in self.disks

    def write(self, disk, key):
        self.disks[disk].write(key)


class Cluster(object):

    def __init__(self, nodes_count=10):
        # self.name = name
        self.nodes = {}

        for _ in xrange(nodes_count):
            nname = rand_str(10)
            self.nodes[nname] = Node(nname)

        self.ring = ConsistentHashRing()
        for _, node in self.nodes.items():
            for dname, disk in node.disks.items():
                self.ring[dname] = disk

    def write(self, key):
        disks = self.ring[key]
        for disk in disks:
            disk.write(key)

    def find_node(self, disk):
        return next(
            (n for _, n in self.nodes.items() if disk in n.disks),
            None
        )

    def get_replicas(self, key):
        d1, d2, d3 = self.ring[key]
        return (
            self.find_node(d1.name),
            self.find_node(d2.name),
            self.find_node(d3.name)
        )


def stats(keys_count=1000000):

    def by_disks():
        result = {}
        for _, disk in cluster.ring.partitions.iteritems():
            result[disk.name] = len(disk.keys) / float(keys_count)
        return result

    def by_nodes():
        result = {}
        for _, disk in cluster.ring.partitions.iteritems():
            node = cluster.find_node(disk.name)
            v = result.setdefault(node.name, 0.0)
            v += len(disk.keys) / float(keys_count)
            result[node.name] = v
        return result

    def by_replicas():
        result = {}
        for k in keys:
            nodes = [n.name for n in cluster.get_replicas(k)]
            uniques = set(nodes)

            for u in uniques:
                x, y = result.setdefault(u, (0, 0))
                x += 1
                y += len([i for i in nodes if i == u])
                result[u] = x, y

        return result

    cluster = Cluster()

    keys = [rand_str(32) for _ in range(keys_count)]
    for key in keys:
        cluster.write(key)

    return by_disks(), by_nodes(), by_replicas()


if __name__ == '__main__':
    by_disks, by_nodes, by_replicas = stats()

    print 'Disks'

    for dname, v in by_disks.items():
        print '\t{}: {}'.format(dname, v)

    print '\nNodes'

    for nname, v in by_nodes.items():
        print '\t{}: {}'.format(nname, v)

    print '\nReplicas'

    for nname, v in by_replicas.items():
        print '\t{}: {}'.format(nname, v[0] / float(v[1]))
