from random import shuffle

s = set()
for i in range(10):
	s.add(i)

print("before suffle", s)
print("before suffle (to make sure)", s)
print("before suffle", shuffle(list(s)))