"""Node class for Distributed Systems Project 1."""

from Event import Event
from Appointment import Appointment, is_appointments_conflicting

class Node(object):
    """
    Node object.

    id:             unique id of this Node; the creator of the Node is
                    responsible for ensuring the id is unique.
    clock:          local clock of the Node enforced as an integer and
                    incremented whenever it is referenced.
    calendar:       local calendar of events maintained as a dictionary
                    data structure by this Node.
    log:            local log of event records maintained by this Node.
    T:              this Node's 2D Time Table.
    node_count:     number of total Nodes in the distributed system enforced as
                    an integer; node_id must be strictly less than this. 
    
    Node ID's are assumed to start at 0.
    """

    def __init__(self, node_id, node_count):
        """Initialize a new Node object."""
        if not isinstance(node_id, int):
            raise TypeError("node_id parameter must be of type int.")
        if not isinstance(node_count, int):
            raise TypeError("node_count parameter must be of type int.")
        
        if node_id > node_count - 1:
            raise ValueError(
                "node_id can not exceed total number of Nodes (node_count)")

        self._id = node_id
        self._clock = 0
        self._calendar = {}
        self._log = []
        self._T = [[0 for j in range(node_count)] for i in range(node_count)]
        self._node_count = node_count
    
    def hasRec(self, eR, k):
        """
        hasRec predicate presented in paper.
        
        Determine if this Node knows that Node k has learned of all events at
        eR.node up until time eR.time.
        """

        #type checking to be safe        
        if not isinstance(eR, Event):
            raise TypeError("eR must be of type Event")
        if not isinstance(k, int):
            raise TypeError("k must be the integer id of some node")

        if k > self._node_count - 1:
            raise ValueError(
                "k must be within range ""[0:" + str(self._node_count-1) + "]")

        return self._T[k][eR._node_id] >= eR._time

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

        We can use name to determine if X is within local calendar by
        assumption of unique names.
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
        pass

    def insert(self, X):
        """Insert Appointment X into this Node's local calendar and log."""
        #ensure we're inserting an Appointment before anything.
        if not isinstance(X, Appointment):
            raise TypeError("X must be of type Appointment.")

        #set i for convenience
        i = self._id

        #TODO: find out if clock only gets updated in event of successful insertion
        #increment clock and update this Node's time table
        self._clock += 1
        self._T[i][i] = self._clock

        #if the appointment doesn't conflict with anything currently in the
        #local calendar
        if not self._is_calendar_conflicting(X):
            
            #create Event object for the insertion of this appointment
            #and place it in the log if it's not in the log already
            e = Event(
                op="INSERT", 
                time=self._clock, 
                node_id=i, 
                op_params=X)

            if e not in self._log:
                self._log.append(e)

            #add appointment to calendar using appointment name as key as
            #we have assumed unique names for appointments.
            self._calendar[X._name] = X

            #for every user in the participant list of scheduled Appointment X
            for user in X._participants:
                #if the user is not this Node, propogate scheduled Appointment
                if user != i:
                    pass#call send with this nodes log + 2DTT

        else:
            #event conflicts with local calendar, 
            #execute conflict resolution protocol
            print "NOPE:" + X._name
            self._handle_conflict()

    def delete(self, X):
        """Insert Appointment X into this Node's local calendar and log."""
        #ensure we're getting an Appointment object or name of one
        if not isinstance(X, Appointment) and not isinstance(X, str):
            raise TypeError("X must be of type or string.")

        i = self._id

        #increment clock and update this Node's time table
        self._clock += 1
        self._T[i][i] = self._clock

        #if the Appointment object (or appointment name) X is in this Node's
        #local calendar
        if self._is_in_calendar(X):
            
            #ensure we store the Appointment itself in op_params and not just
            #the name of some Appointment object
            if isinstance(X, Appointment):
                appt = X
            else:
                appt = self._calendar[X]

            #create Event object for the deletion of this appointment
            #and place it in the log if it's not in the log already
            e = Event(
                op=r"DELETE", 
                time=self._clock, 
                node_id=i, 
                op_params=appt)

            if e not in self._log:
                self._log.append(e)

            #add appointment to calendar using appointment name as key as
            #we have assumed unique names for appointments.
            self._calendar.pop(X._name, None)

            #for every user in the participant list of scheduled Appointment X
            for user in X._participants:
                #if the user is not this Node, propogate scheduled Appointment
                if user != i:
                    pass#call send with this nodes log + 2DTT

    def send(self, k):
        """Build partial log and send to node with node_id k."""
        import copy
        #construct partial log of events to send to Node k
        NP = [eR for eR in self._log if not self.hasRec(eR, k)]
        msg = (NP, copy.deepcopy(self._T), self._node_id)

        #do send of actual msg via TCP

    def receive(self):
        """Receive messages over TCP."""

        #set i and n for name convenience
        i, n = self._node_id, self._node_count
        
        #dummy message for now
        m = (None, None)

        #pull partial log, 2DTT and sender id k from message m
        NPk, Tk, k = m

        #get list of events this Node doesn't know about
        NE = [fR for fR in NPk if not self.hasRec(fR, i)]

        #get list of Appointments within this Node's calendar and the
        #Appointments from the NE list
        Vi = list(set(self._calendar.values() + [cvR.op_params for cvR in NE]))

        filtered_Vi = []

        #filter out deleted Appointments
        for v in Vi:
            for dR in NE:
                if dR.op == r"DELETE" and dR.op_params == v:
                    break
            else:
                filtered_Vi.append(v)

        #list of appointments which will become this Node's new dict
        Vi = filtered_Vi
        #rewrite dictionary with valid appointments only
        self._calendar = {}
        for v in Vi:
            self._calendar[v._name] = v

        #extract direct knowledge from Node k's 2DTT
        for I in range(n):
            self._T[i][I] = max(self._T[i][I], Tk[k][I])

        #extract indirect knowledge from Node k's 2DTT
        for I in range(n):
            for J in range(n):
                self._T[I][J] = max(self._T[I][J], Tk[I][J])

        #create new log and union of this Node's log and NE list
        new_log = []
        PLiUNE = list(set(list(self._log) + NE))
        #if there is some Node j for which this Node knows j does not know of
        #all events up to time eR.time, we can't discard it, keep it in the log
        for eR in PLiUNE:
            for j in range(n):
                if not self.hasRec(eR, j):
                    new_log.append(eR)
                    break

        self._log = new_log
