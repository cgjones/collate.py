#!/usr/bin/python

#
# Copyright (c) 2013 Chris Jones
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

##
## Given source data files that look like
##
##  1.00 Expense X
##  2.00 Expense Y
##  +10.00 Income
##
## perform one of a set of analyses on the data files and print the
## results.
##

class Group:
    '''Consists of a label and a set of "children" groups.  A group's
    label can be |None| to indicate an anonymous group.  Each child is
    either a recursive Group, or a leaf string.'''
    def __init__(self, label, *kids):
        self.label = label      # str or None
        self.kids = kids        # [ Group or str ]

##
## File that defines the spec for the "sort" command below.  Format is
## like the following
##
##  Group(None,
##        Group('A',
##              'Leaf1', 'Leaf2'),
##        Group('RecursiveB',
##              Group('C',
##                    'Leaf3', 'Leaf4'),
##              'Leaf5')
##       )
##
GROUPS = eval(open('g.groups').read())

class Item:
    '''Something with a label and amount.'''
    def __init__(self, label, amount=0.0):
        self.label = label      # str or None
        self.amount = amount    # float

    def __repr__(self):
        return '<Item %s amount=%g>'% (self.label, self.amount)

class Node:
    '''A node in a dataflow tree.  Consists of an Item that represents
    this node, along with an optional parent label (string) that's the
    label of the group this node belongs to.'''
    def __init__(self, tree, label, parent=None):
        self.tree = tree        # str -> Node
        self.item = Item(label) # Item
        # XXX make me an array if we need a dag
        self.parent = parent    # str or None

    def notify(self, amount):
        '''Add |amount| to this node and notify the parent.'''
        self.item.amount += amount
        if self.parent:
            self.tree[self.parent].notify(amount)

def dataflow_of_group(g):
    '''Return the dataflow tree for the group tree |g|, that is, the
    edges along which reactive changes will propagate.'''
    def recurse(parent, group, edges, seen):
        if group.label:
            parent = group.label
        for kid in group.kids:
            if isinstance(kid, Group):
                recurse(parent, kid, edges, seen)
                kid = kid.label

            assert isinstance(kid, str)
            assert not (kid in seen)

            edges[kid] = Node(edges, kid, parent)
            seen.add(kid)
        return edges

    return recurse(None, g, { }, set())

def dump_group_tree(g, t, depth=0):
    anon = g.label is None
    pfx = ';' * (2 * depth)

    '''Dump the dataflow tree |t| (built from |g|) as a set of
    semi-colon separated pairs "label;amount".  The data are formatted
    so as to be imported by a spreadsheet program.'''
    def print_item(i):
        print '%s%s;%.2f'% (pfx, i.label, i.amount)
    def print_blank():
        print '%s;'% (pfx)

    if not anon:
        total = t[g.label].item
        print '%s-----;-----'% (pfx)
        print_item(total)
        print_blank()
        for kid in g.kids:
            kid = kid if isinstance(kid, str) else kid.label
            print_item(t[kid].item)
        for i in xrange(15):
            print_blank()

    for kid in g.kids:
        if isinstance(kid, Group):
            dump_group_tree(kid, t, depth + 1 if not anon else depth)

def accum_line(fn, line, accum):
    '''Parse |line| into the match m and call |fn(m.groups(), accum)|.'''
    import re

    m = re.match(r'([+]?)(\d+(?:\.\d*)) (.*)', line)
    if m:
        deposit, amount, label = m.groups()
        amount = float(amount)
        fn(Item(label, -amount if not deposit else amount), accum)

def accum_fd(fn, fd, accum):
    '''Call |fn(m.groups(), accum)| for each line in the file object
    |fd| that's successfully parsed.'''
    for line in fd:
        accum_line(fn, line, accum)

def usage(argv):
    print>>sys.stderr, ('Usage: python %s <analysis> file>...')% (argv[0])

class Analyses:
    '''Commands for analyzing a data set.  Each command is defined as
    an accumulator function.  The first time the function is called,
    the accumulator param is None.  The function must create an
    accumulator and return it.  Successive calls pass the parsed Item
    and the accumulator object.  The last call is made with a None
    Item and the accumulator.  The function should finalize its
    analysis then (usually printing summary results).'''
    @staticmethod
    def echo(item, _):
        '''Print |item|.'''
        if item:
            print item

    @staticmethod
    def sum(item, accum):
        '''For each item label |l| seen, sum all items seen with |l|.
        Finally, print all the sums.'''
        if accum is None:
            return { }
        if item is None:
            for i in accum.itervalues():
                print '%s,%g'% (i.label, i.amount)
            return

        i = accum.get(item.label)
        if not i:
            i = Item(item.label, 0)
            accum[i.label] = i
        i.amount += item.amount

    @staticmethod
    def sort(item, accum):
        '''Accumulate a tree of partial sums based on the spec
        |GROUPS|.  Finally, dump the tree of partial sums.'''
        if accum is None:
            return dataflow_of_group(GROUPS)
        if item is None:
            return dump_group_tree(GROUPS, accum)

        accum[item.label].notify(item.amount)

def main(argv):
    if 3 > len(argv):
        usage(argv)

    analysis = getattr(Analyses, argv[1])
    accum = analysis(None, None)
    for f in argv[2:]:
        accum_fd(analysis, open(f), accum)
    analysis(None, accum)

if __name__ == '__main__':
    import sys
    main(sys.argv)
