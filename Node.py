"""Node class for Distributed Systems Project 1."""

from Event import Event
from Appointment import Appointment, is_appointments_conflicting

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
      
    def _is_calendar_conflicting(self, X):
        """
        Determine if Appointment object X conflicts with some Appointment
        already in the calendar.
        """

        #for each appointment in the calendar
        for name, appointment in self._calendar.iteritems():
            #if any appointment conflicts with new appointment X, they conflict
            if is_appointments_conflicting(appointment, X):
                return True

        return False

    def _is_in_calendar(self, X):
        """
        Determine if X (an Appointment or string) is within this Node's
        calendar.
        """

        #determine if X is a string or Appointment, raise TypeError if neither
        is_name_string = isinstance(X, str)
        is_appointment = isinstance(X, Appointment)

        if not is_name_string and not is_appointment:
            raise TypeError(str(X) + " must be a string or Appointment object")

        #if X is a string
        if is_name_string:
            #determine if X is the name of any entry in this Node's calendar
            for appointment_name in self._calendar.keys():
                if appointment_name == X:
                    return True

        #if X is an Appointment object
        if is_appointment:
            #determine if name of X is the name of any entry in this Node's
            #calendar
            for appointment_name in self._calendar.keys():
                if X._name == appointment_name:
                    return True

        return False

    def _handle_conflict(self):
        """Execute conflict resolution protocol."""
        print "conflict"
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


        '''
        #if the appointment doesn't conflict with anything currently in the
        #local calendar
        if not self._is_calendar_conflicting(X):
            
            #create Event object for the insertion of this appointment
            #and place it in the log if it's not in the log already
            e = Event(
                op="INSERT", 
                time=self._clock, 
                node_id=self._id, 
                op_params=X)

            if e not in self._log:
                self._log.append(e)

            #add appointment to calendar using appointment name as key as
            #we have assumed unique names for appointments.
            self._calendar[X._name] = X
        else:
            #event conflicts with local calendar, 
            #execute conflict resolution protocol
            self._handle_conflict()
        '''
