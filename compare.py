import sys

f1 = open(sys.argv[1]).readlines()
f2 = open(sys.argv[2]).readlines()

s1, s2 = set(), set()
matches = set()
for l1 in f1:
	if l1.startswith("score"):
		continue
	l1 = set(l1.split()[1].strip("()").split(",")[:2])
	s1.add(tuple(l1))
	for l2 in f2:
		if l2.startswith("score"):
			continue
		l2 = set(l2.split()[1].strip("()").split(",")[:2])
		s2.add(tuple(l2))
		
		if l1 == l2:
			matches.add(tuple(l1))
			
print len(s1), len(s2), len(matches), len(matches) / float(len(s1))

