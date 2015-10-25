"""Node class for Distributed Systems Project 1."""

import sys
import socket
import thread
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
    node_ID_to_IP   Dictionary of form [Int: (String1, String2)], containing
                    the NodeIDs to IP address relationship of all nodes in
                    system. String1 is the IP while String2 is the port number                

    Node ID's are assumed to start at 0.
    """

    def __init__(self, node_id, node_count, ids_to_IPs):
        """Initialize a new Node object."""
        if not isinstance(node_id, int):
            raise TypeError("node_id parameter must be of type int.")
        if not isinstance(node_count, int):
            raise TypeError("node_count parameter must be of type int.")
        if not isinstance(ids_to_IPs, dict):
            raise TypeError("ids_to_IPs must be of type dictionary.")
        
        if node_id > node_count - 1:
            raise ValueError(
                "node_id can not exceed total number of Nodes (node_count)")

        self._id = node_id
        self._clock = 0
        self._calendar = {}
        self._log = []
        self._T = [[0 for j in range(node_count)] for i in range(node_count)]
        self._node_count = node_count
        self._ids_to_IPs = ids_to_IPs
    
    def __str__(self):
        """Human readable string of this Node."""
        hr_str = ""
        hr_str += "ID:" + str(self._id) + '\t\t' + str(type(self._id)) + '\n'
        hr_str += "CLOCK: " + str(self._clock) + '\t' + str(type(self._clock)) + '\n'
        hr_str += "CALENDAR:\n"
        for k,v in self._calendar.iteritems():
            hr_str += "\tAPPOINTMENT:" + k + '\t' + str(type(v)) + '\n'
        hr_str += "LOG:\n"
        for eR in self._log:
            hr_str += '\t' + str(eR) + '\t' + str(type(eR)) + '\n'

        hr_str += "TIME TABLE:\n"
        for row in self._T:
            hr_str += '\t' + str(row) + '\n'
        hr_str += "NODE COUNT:" + str(self._node_count) + '\n'
        return hr_str
    
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

    def _is_calendar_conflicting(self, X, other_calendar=None):
        """
        Determine if Appointment object X conflicts with some Appointment
        already in the calendar.
        """

        if other_calendar:
            calendar = other_calendar
        else:
            calendar = self._calendar

        #for each appointment in the calendar
        for name, appointment in calendar.iteritems():
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

    def _handle_conflict(self, X):
        """Execute conflict resolution protocol."""
        self.delete(X)

    def _load_state(self):
        """Load a previous state of this Node."""
        import pickle
        N = pickle.load( open( "state.p", "rb" ) )
        self._id = N._id
        self._clock = N._clock
        self._calendar = N._calendar
        self._log = N._log
        self._T = N._T
        self._node_count = N._node_count

    def _save_state(self):
        """Save this Node's state to state.txt in cwd."""
        import pickle
        pickle.dump( self, open( "./state.p", "wb" ) )

    def insert(self, X):
        """Insert Appointment X into this Node's local calendar and log."""
        #ensure we're inserting an Appointment before anything.
        if not isinstance(X, Appointment):
            raise TypeError("X must be of type Appointment.")
 

        #if the appointment doesn't conflict with anything currently in the
        #local calendar
        if not self._is_calendar_conflicting(X):
            
            #set i for convenience
            i = self._id
            #TODO: find out if clock only gets updated in event of successful insertion
            #increment clock and update this Node's time table
            self._clock += 1
            self._T[i][i] = self._clock

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
                    try:
                        thread.start_new_thread(self.send(user))
                    except:
                        pass

        else:
            #event conflicts with local calendar, 
            #execute conflict resolution protocol
            print "NOPE:" + X._name
            self._handle_conflict(X)

    def delete(self, X):
        """Insert Appointment X into this Node's local calendar and log."""
        #ensure we're getting an Appointment object or name of one
        if not isinstance(X, Appointment) and not isinstance(X, str):
            raise TypeError("X must be of type or string.")

        #if the Appointment object (or appointment name) X is in this Node's
        #local calendar
        if self._is_in_calendar(X):

            i = self._id

            #increment clock and update this Node's time table
            self._clock += 1
            self._T[i][i] = self._clock
            
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
                #if the user is not this Node, propogate canceled Appointment
                if user != i:
                    try:
                        thread.start_new_thread(self.send(user))
                    except:
                        pass

    def send(self, k):
        """Build partial log and send to node with node_id k."""
        import copy
        #construct partial log of events to send to Node k
        NP = [eR for eR in self._log if not self.hasRec(eR, k)]
        msg = (NP, copy.deepcopy(self._T), self._id)

        #do send of actual msg via TCP
        ip_port_K = self._ids_to_IPs[k]

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip_port_K[0], ip_port_K[1]))

        #pickle message and send
        import pickle
        message = pickle.dumps(msg)
        sock.send(message)
        sock.close()

    def receive(self, message):
        """Receive messages over TCP."""

        #unpickle message
        import pickle
        m = pickle.loads(message)

        #set i and n for name convenience
        i, n = self._id, self._node_count

        #pull partial log, 2DTT and sender id k from message m
        NPk, Tk, k = m

        #get list of events this Node doesn't know about
        NE = [fR for fR in NPk if not self.hasRec(fR, i)]

        #get list of Appointments within this Node's calendar and the
        #Appointments from the NE list
        Vi = list(
            set(self._calendar.values() + [cvR._op_params for cvR in NE]))

        filtered_Vi = []

        #filter out deleted Appointments
        for v in Vi:
            for dR in NE:
                if dR._op == r"DELETE" and dR._op_params == v:
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

        return Ne

    def parse_command(self, cmd):
        """
        Parse schedule, cancel and fail commands.

        If provided command is wrongly formatted, "Invalid command: [reason]"
        is printed to console and command is not processed.

        [user assigning action] [appointment_type] [appointment_name] [ ( tuple of users appointment is for) ] [ (startTime, endtime) ] [ Day ]
        ex. "user1 schedules yaboi (user0,user1,user2,user3) (4:00pm,6:00pm) Friday".
        """

        def create_arguements(cmd_string):
            """Split arguements into componenets."""
            parts_of_appointment = cmd_string.split(" ")
            #define error string
            error_str = "[ERROR]: Command has too {modal} arguements 6 required, "
            error_str += (str(len(parts_of_appointment)) + " provided.")

            #if correctly formatted
            if len(parts_of_appointment) == 6:
                return [part for part in parts_of_appointment]
            else:
                #print error; invalid format
                if len(parts_of_appointment) > 6:
                    print error_str.format(modal="many")
                elif len(parts_of_appointment) < 6:
                    print error_str.format(modal="little")
                
                return

        def generate_appointment(args):
            """Convert provided args into an Appointment object."""
            #split arg into relevant fields
            name, participants, times, day = args[2], args[3], args[4], args[5]

            #convert list of participants into list of integer node id's
            participants = participants[1:-1].replace("user", "").split(',')
            node_ids = [int(p) for p in participants]

            #split start and end time
            start_time, end_time = times[1:-1].split(',')

            #generate and return appointment object
            X = Appointment(
                str(name), str(day), str(start_time), str(end_time), node_ids)
            return X

        def handle_schedule(cmd):
            """Handle scheduling."""
            X = generate_appointment(cmd)
            if X:
                print "insert happened"
                self.insert(X)

        def handle_cancel(cmd):
            """Handle cancellations."""
            X = generate_appointment(cmd)
            if X:
                self.delete(X)
            #print "cancel: " + str(X)

        def handle_fail(cmd):
            """Handle failures."""
            self._save_state()

        args = create_arguements(cmd)

        if args:
            command_type = args[1]

            if command_type == "schedules":
                handle_schedule(args)
            elif command_type == "cancels":
                handle_cancel(args)
            elif command_type == "fail":
                handle_fail(args)
            else:
                print "[ERROR]: Command Type not correct. use 'schedules','cancels', or 'fail' "
                pass

        '''
        import re
        schedule_pattern = "user \d+ schedules appointment"
        cancel_pattern = "user \d+ cancels appointment"
        fail_pattern = "user \d+ (fails|crashes|goes down)$"


        print "cmd:", cmd
        if re.match(schedule_pattern, cmd):
            handle_schedule(cmd)
        elif re.match(cancel_pattern, cmd):
            handle_cancel(cmd)
        elif re.match(fail_pattern, cmd):
            handle_fail(cmd)
        else:
            print "Invalid command: \"" + str(cmd) + "\""
        '''

    @staticmethod
    def _using_conflict_resolution_protocol(message):
        """Determine if messgae is from conflict resolution protocol."""
        #pull partial log, 2DTT and sender id k from message m
        NPk, Tk, k = message

        #if any value is not 0, the message was sent through Wuu-Bernstein
        for row in Tk:
            for element in row:
                if element != 0:
                    return False
        return True

def client_thread(conn, Node):
    """."""
    while 1:
        data = conn.recv(2048)

        if not data:
            print("Ended connection")
            break

        if data.decode("utf-8") == "terminate" or data.decode("utf-8") == "quit":
            print("Ending connection with client")
            conn.close()
            break

        #copy the calendar before receiving data i.e. redefining this Node's
        #calendar
        from copy import deepcopy
        pre_dict = deepcopy(Node._calendar)
        dict_entries = Node.receive(data)
        #Get the appointments in new dictionary not in old dictionary
        new_entries = [entry for entry in dict_entries if entry not in pre_dict.values()]
        #for each new appointment entry, if it's conflicting, handle it 
        for entry in new_entries:
            if Node._is_calendar_conflicting(entry, pre_dict):
                Node._handle_conflict(entry)


        conn.send(b'ACK ' + data)
    conn.close()

def main():
    """Main method; listener for input housed here."""

    '''
    cmd1 = "user1 schedules yaboi (user0,user1,user2,user3) (4:00pm,6:00pm) Friday"
    cmd2 = "user1 schedules gay (user0,user1,user2,user3) (4:00pm,6:00pm) Sunday"
    cmd2 = "user1 schedules straight (user0,user1,user2,user3) (2:00pm,4:00pm) Sunday"
    cmd1 = "user1 cancels yaboi (user0,user1,user2,user3) (4:00pm,6:00pm) Friday"
    cmd2 = "user1 schedules just_guys (user0,user1,user2,user3) (1:00am,6:30am) Tuesday"
    cmd3 = "user1 schedules test (user1,user2,user3) (3:00am,6:00am) Tuesday"
    cmd3 = "user1 schedules new (user0,user1,user2,user3) (1:00pm,1:30pm) Thursday"
    '''
    
    Virginia_IP = "52.91.70.98"
    Oregon_IP = "52.88.140.4"
    California_IP = "54.67.83.210"
    Ireland_IP = "52.17.138.211"

    #init IP's of different regions
    ids_to_IPs = {
        0: (Virginia_IP, 9000),
        1: (Oregon_IP, 9001),
        2: (California_IP, 9002), 
        3: (Ireland_IP, 9003)}

    N = Node(node_id=0, node_count=4, ids_to_IPs=ids_to_IPs)

    #try to load a previous state of this Node
    try:
        N._load_state()
    except IOError:
        pass

    HOST = "0.0.0.0"
    PORT = int(sys.argv[1])

    #bind to host of 0.0.0.0 for any TCP traffic through AWS
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((HOST, PORT))
    #backlog to; 1 for each process
    sock.listen(4)

    import select
    print("@> Node Started")
    while True:
        r, w, x = select.select([sys.stdin, sock], [], [])
        if not r:
            continue
        if r[0] is sys.stdin:
            message = raw_input('')
            if message == "quit":
                N._save_state()
                break
            elif message == "log":
                print str(N)
            else:
                N.parse_command(message)
        else:
            conn, addr = sock.accept()
            print ('Connected with ' + addr[0] + ':' + str(addr[1]))
            thread.start_new_thread(client_thread ,(conn, N))
    sock.close()
    
if __name__ == "__main__":
    main()
