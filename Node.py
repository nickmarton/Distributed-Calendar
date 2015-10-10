"""Node class for Distributed Systems Project 1."""

from Event import Event
from Appointment import Appointment, is_conflicting

class Node(object):
    """
    Node object.

    node_count:     number of total Nodes in the distributed system enforced as
                    an integer; node_id must be strictly less than this. 
    node_id:        unique id of this Node; the creator of the Node is
                    responsible for ensuring the id is unique.
    clock:          local clock of the Node enforced as an integer and
                    incremented whenever it is referenced.
    calendar:       local calendar of events maintained as a dictionary
                    data structure by this Node.
    log:            local log of event records maintained by this Node.
    T:              this Node's 2D Time Table.
    """

    def __init__(self, node_id, node_count):
        """Initialize a new Node object."""
        if not isinstance(node_id, int):
            raise TypeError("node_id parameter must be of type int.")
        if not isinstance(node_count, int):
            raise TypeError("node_count parameter must be of type int.")
        
        if node_id > node_count:
            raise ValueError(
                "node_id can not exceed total number of Nodes (node_count)")

        self._id = node_id
        self._clock = 0
        self._calendar = {}
        self._log = []
        self._T = [[0 for j in range(node_count)] for i in range(node_count)]
        
    def _handle_conflict(self):
        """Execute conflict resolution protocol."""
        pass

    def insert(self, X):
        """Insert Appointment X into this Node's local calendar and log."""
        #ensure we're inserting an Appointment before anything.
        if not isinstance(X, Appointment):
            raise TypeError("X must be of type Appointment.")

        #TODO: find out if clock only gets updated in event of successful insertion
        #increment clock and update this Node's time table
        self._clock += 1
        self._T[self._id][self._id] = self._clock

        #if the event doesn't conflict with anything currently in the
        #local calendar
        if not self._is_conflicting(X):
            
            #add appointment to calendar using appointment name as key as
            #we have assumed unique names for appointments.
            self._calendar[X._name] = X

            #create Event object for the insertion of this appointment
            #and place it in the log if it's not in the log already
            e = Event("INSERT", self._clock, self._id, X)
            if e not in self._log:
                self._log.append(e)
        else:
            #event conflicts with local calendar, 
            #execute conflict resolution protocol
            self._handle_conflict()