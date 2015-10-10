"""Distributed Systems Project 1."""

from Appointment import Appointment
from Node import Node

def main():
    """Main method."""

    num_nodes = 4

    a1 = Appointment("yo","saturday","12:30pm","1:30pm", [1, 2, 3])
    a2 = Appointment("yerboi","saturday","12:30am","12:00pm", [1, 4, 5])
    a3 = Appointment("we out here","saturday","11:30am","12:30pm", [1])

    N1 = Node(node_id=0, node_count=num_nodes)
    N1.insert(a1)
    print
    N1.insert(a2)
    #N1.insert(a3)
    for i in N1._log:
        print i

if __name__ == "__main__":
    main()