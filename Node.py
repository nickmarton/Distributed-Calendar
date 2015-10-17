"""Node class for Distributed Systems Project 1."""

from Event import Event
from Appointment import Appointment, is_appointments_conflicting
import socket
import thread

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
    node_ID_to_IP   Dictionary of form [Int: (String1, String2)], containing the NodeIDs
                    to IP address relationship of all nodes in system. String1 is the IP 
                    while String2 is the port number                

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

    def _save_state(self):
        """Save this Node's state to state.txt in cwd."""
        #test nested logs
        '''
        log = [eR for eR in self._log]
        T = [[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4]]
        X = (log, T)

        e = Event(
            op="SEND", 
            time=self._clock, 
            node_id=self._id, 
            op_params=X)


        self._log.append(e)
        log2 = [eR for eR in self._log if eR._op == "SEND"]
        self._log = self._log[:-1]
        T2 = [[6,7,8],[6,7,8],[6,7,8],[6,7,8]]
        X2 = (log2, T2)

        e2 = Event(
            op="RECEIVE", 
            time=self._clock, 
            node_id=self._id, 
            op_params=X2)

        self._log.append(e2)
        for i in self._log:
            print i._op_params
        '''

        with open("./state.txt", "w") as f:
            #write id and clock value on separate lines
            f.write(str(self._id) + '\n')
            f.write(str(self._clock) + '\n')
            #write appointments to file each on their own line
            for appt in self._calendar.values():
                f.write(appt.__repr__() + '\n')
            #write appointments to file each on their own line
            for eR in self._log:
                f.write(eR.__repr__() + '\n')
            #construct single line rep. of table; the table is square so
            #easily recoverable
            table_str = ''
            for row in self._T:
                 table_str += '_'.join([str(i) for i in row])
                 table_str += '_'
            f.write(table_str[:-1] + '\n')
            #write node count
            f.write(str(self._node_count) + '\n')

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
        msg = (NP, copy.deepcopy(self._T), self._id)

        #do send of actual msg via TCP

        ip_port_K = self._ids_to_IPs[k]

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip_port_K[0], ip_port_K[1]))

        sock.send(str(msg))

    def receive(self, message):
        """Receive messages over TCP."""

        print(message.decode("utf-8"))
        #set i and n for name convenience
        i, n = self._node_id, self._node_count
        
        #dummy message for now

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

    def parse_command(self, cmd):
        """
        Parse schedule, cancel and fail commands.

        If provided command is wrongly formatted, "Invalid command: [reason]"
        is printed to console and command is not processed.




        [user assigning action] [appointment_type] [appointment_name] [ ( tuple of users appointment is for) ] [ (startTime, endtime) ] [ Day ]

        user1 schedules yaboi (user1,user2,user3) (4:00pm,6:00pm) Friday

        """

        def create_arguements(cmd_string):
            parts_of_appointment = cmd_string.split(" ")

            if len(parts_of_appointment) == 6:
                ans = []
                for part in parts_of_appointment:
                    ans.append(part)
                return ans

        def generate_appointment(cmd):
            acting_user = cmd[0]
            appointment_type = cmd[1]
            appointment_name = cmd[2]
            users_involved = cmd[3]
            time_of_appointment = cmd[4]
            day_of_appointment = cmd[5]

            #put all users involved into a list
            users_involved = users_involved.split(',')
            node_ids = []
            for user in users_involved:
                current_user = user
                current_user = current_user.strip('(')
                current_user = current_user.strip(')')
                current_user = current_user.strip('user')
                node_ids.append(int(current_user))

            start_end_times = time_of_appointment.split(',')

            start_time = start_end_times[0].strip("(")
            end_time = start_end_times[1].strip(")")



            x = Appointment( 'user' + str(acting_user), str(day_of_appointment), str(start_time), str(end_time), node_ids)

            return x

        '''
        def generate_appointment(cmd, cmd_key):
            """Generate appointment for schedule and cancel commands."""
            error_msg = "command must in form: [user + cmd + \"appointment\"]"
            error_msg += " for [participants] for [start_time - end_time]"
            try:
                #separate user, participants and time
                user, participants, time = cmd.split(" for ")


                print "user:", user, " participants:", participants, " time:", time 
                #get scheduler
                scheduler = int(user[5:user.find(cmd_key)])
                
                #get integer list of participant id's and check if each id is
                #valid; remove all characters except digits, commas, and single
                #whitespace (provided whitespace and commas not leading/trailing)
                error_msg = "all participants id's must be integers separated "
                error_msg += "by ' and ' or ', ' with no trailing commas."
                participants = re.sub("[^\d, ]", "", participants).strip()
                participants = list(set(
                    [int(p) for p in participants.split(",")]))

                #ensure all participant id's are valid
                for p in participants:
                    if p >= self._node_count:
                        error_msg = "Participant id's must be less than the"
                        error_msg += "total number of nodes"
                        raise ValueError()

                #split times from day if day provided, defaults to Sunday
                day = "Sunday"
                if " on " in time:
                    time, day = time.split(" on ")

                #split start and end times and reformat
                error_msg = "times provided for appointment must be of the "
                error_msg += "form: (digit){1,2}(|:(digit){2})(am|pm) - "
                error_msg += "(digit){1,2}(|:(digit){2})(am|pm)"
                start_time, end_time = time.replace(" ", "").split("-")

                #print "Start time:", start_time, " end_time :", end_time
                
                if ":" not in start_time:
                    start_time = start_time[:-2] + ":00" + start_time[-2:]
                if ":" not in end_time:
                    end_time = end_time[:-2] + ":00" + end_time[-2:]

                #if the last word before first "for" isn't appointment, treat
                #everything after appointment as the name for the appointment
                #the user is scheduling, otherwise build unique name from
                #appointment info
                if user.split(" ")[-1] != "appointment":
                    index = -1
                    for i, word in enumerate(cmd.split(" ")):
                        if word == "appointment":
                            index = i + 1
                            break
                    name = ' '.join(user.split(" ")[index:])
                else:
                    #build unique name of form
                    #"[scheduler]_[participants]_[start]_[end]_[day]"
                    name = str(scheduler) \
                    + "_" + ",".join([str(p) for p in participants]) \
                    + "_" + start_time + "_" + end_time + "_" + day

                X = Appointment(name, day, start_time, end_time, participants)
                return X
            except Exception as e:
                print "Invalid command: \"" + str(error_msg) + "\""
                return None
        '''
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

        arguements = create_arguements(cmd)
        command_type = arguements[1]

        if command_type == "schedules":
            handle_schedule(arguements)
        elif command_type == "cancels":
            handle_cancel(arguements)
        elif command_type == "fail":
            handle_fail(arguements)
        else:
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



def client_thread(conn, Node):
    while 1:
        data = conn.recv(2048)

        if not data:
            print("Ended connection")
            break

        if data.decode("utf-8") == "terminate" or data.decode("utf-8") == "quit":
            print("Ending connection with client")
            conn.close()
            break

        command = data.decode("utf-8")
        print "here\n"
        Node.receive(command)

        conn.send(b'ACK ' + data)
    conn.close()

def main():
    """Main method; listener for input housed here."""



    ids_to_IPs = { 0 : ("52.88.200.87", 1024), 1: ("54.68.99.54",1024)}
    N1 = Node(node_id = 0, node_count = 4, ids_to_IPs = ids_to_IPs)


    HOST = ""
    PORT = 1024

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((HOST, PORT))
    sock.listen(3)

    #N1.parse_command("user 1 schedules appointment yo for users 1,2,3 for 2pm - 3pm on Friday")


    #listen forever
    #while True:
    #    cmd = parse_command(raw_input().lower())

    import select
    import sys
    print("@> Node Started")
    while 1:
        r, w, x = select.select([sys.stdin, sock], [], [])
        if not r:
            continue
        if r[0] is sys.stdin:
            message = raw_input('')
            if message == "quit":
                break
            else:
                N1.parse_command(message)
                N1.send(0)
        else:
            conn, addr = sock.accept()
            print ('Connected with ' + addr[0] + ':' + str(addr[1])) 
            thread.start_new_thread(client_thread ,(conn, N1))
    sock.close()

    
#    N1 = Node(node_id=1, node_count=4)
#    N1.parse_command("user 1 schedules appointment yo for users 1,2,3 for 2pm - 3pm on Friday")
#    N1.parse_command("user 1 schedules appointment we out here for users 1,2,3 for 2pm - 3pm on Saturday")
#    N1.parse_command("user 1 schedules appointment yo2 for users 1,2,3 for 2pm - 3pm")
#    N1.parse_command("user 1 goes down")
    
if __name__ == "__main__":
    main()