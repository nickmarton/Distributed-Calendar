"""Distributed Systems Project 1."""

from Appointment import Appointment
from Node import Node
from Event import Event

def main():
    """Main method."""

    num_nodes = 4

    a1 = Appointment("yo","saturday","12:30pm","1:30pm", [1, 2, 3])
    a4 = Appointment("yo","saturday","12:30pm","1:30pm", [1, 3, 2])
    a2 = Appointment("yerboi","saturday","12:30am","12:00pm", [1, 4, 5])
    a3 = Appointment("we out here","saturday","11:30am","12:30pm", [1])


    N1 = Node(node_id=0, node_count=num_nodes)
    #N1.insert(a1)
    #N1.insert(a1)
    #N1.insert(a3)
    for i in N1._log:
        print i

    e = Event(
        op="INSERT", 
        time=1, 
        node_id=1, 
        op_params=a1)

    e2 = Event(
        op="SEND", 
        time=1, 
        node_id=1, 
        op_params=([e], N1._T))

    print e == e2
    print e
    print e2

if __name__ == "__main__":
    main()
