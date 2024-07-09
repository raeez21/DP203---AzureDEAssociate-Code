str = "Hello"
i =1
num = 20.6
status = True
print(str)
print(i)
print(num)
print(status)

x = [1,2,3,4,5]
print(x)
print(x[3])
x[4] = 100
x.append(200)
print(x)

Courses = ["CourseA","CourseB","CourseC", "CourseD"]
for x in Courses:
    print(x)

num = 10
if num < 10:
    print("less than 10")
else:
    print("equal or more than 10")

while(num > 5):
    print(num)
    num -= 1


def add(x,y):
    return x+y

print("Sum is",add(10,4))