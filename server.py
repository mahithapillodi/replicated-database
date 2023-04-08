#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 20:52:45 2023

@author: miaweaver
"""
from concurrent import futures
import raft_pb2
import raft_pb2_grpc
import grpc
import sys
from random import choice
import time
import signal
import math

import pickle



def set_timer(func, reset):
    global LOG
    LOG[TERM].append("Timer started...")
    if reset:
        signal.alarm(0) #stops current alarm
    heartbeat_timeout = choice(range(100, 200)) ##EDIT THIS..
    signal.signal(signal.SIGALRM, func)
    signal.alarm(heartbeat_timeout)
    return

def stop_timer():
    LOG[TERM].append("Timer stopped...")
    signal.alarm(0)
    return

################################## INTERFACE WITH DB #####################################
#
#
#
##########################################################################################
def set_val(key, val):
    leader_id = None
    global LOCAL_PENDING, REMOTE_PENDING, ID, ELECTIONS, TERM
    if ELECTIONS[max(ELECTIONS.keys())] == ID:
        LOCAL_PENDING[TERM][key] = val
    else:
        leader_id = max(ELECTIONS.keys())
    print(LOCAL_PENDING)
    return (key in LOCAL_PENDING.keys(), leader_id)

def get_val(key):    
    try:
        with open('raft_db.pickle','rb') as handle:
                COMMITTED_DB = pickle.load( handle ) ##load DB
        return COMMITTED_DB[key]
    except:
        return "ERR"

def update_db(key, value):
    print("updating db...")
    global LOCAL_PENDING, TERM
    try:
        with open('raft_db.pickle','rb') as handle:
            COMMITTED_DB = pickle.load( handle ) ##load DB
    except:
        COMMITTED_DB = {}
        
    if TERM in LOCAL_PENDING.keys():
        if key in LOCAL_PENDING[TERM].keys():
            COMMITTED_DB[key] = LOCAL_PENDING[TERM][key]
    
    print("COMMITED_DB...", COMMITTED_DB)
    with open('raft_db.pickle','wb+') as handle:
        pickle.dump( COMMITTED_DB, handle, protocol=pickle.HIGHEST_PROTOCOL ) ##load DB
    print("saves written...")
    return key in COMMITTED_DB


######################## FCNS TO HANDLE RPC INVOKING STATE CHANGE ########################
#
#
#
##########################################################################################
def handle_vote_request(request_term):
    print("handlng vote_request...")
    global PENDING_ELECTION, VOTED, TERM
    
    if request_term > TERM:
        VOTED = False ##have not yet voted in this term...
        TERM = request_term
        
    if not VOTED :
        VOTED = True
        PENDING_ELECTION = True
        print("voted...")
        return True
    print("not voting...")
    return False

def handle_appendEntry(key, value):
    LOCAL_PENDING[key] = value
    return LOCAL_PENDING

## ELECTION HANDLER FUNCTIONS:
#  election_lost():
#       called in case of heartbeat timeout, or upon rep db initialization, if
#       db initialized with no leader
def election_lost(leader_identified = True): ##invoked when dispatcher in candidate state and receives heartbeat
    print("ELECTION LOST...")
    global STATE, PENDING_ELECTION, VOTES, VOTED, RESET_TIMER, SET_TIMER
    if leader_identified:
        VOTED = False
        PENDING_ELECTION = False #stop the election
    STATE = "F" #update state of dispatcher back to follower
    VOTES = [] ##re-initialize votes for next election
    RESET_TIMER, SET_TIMER = False, True ##flag to set timer...
    #set_timer( invoke_election, reset = False) ##initialize heartbeat timer
    print("STATE:", STATE)
    return

#  election_timeout():
#       called when election times out; simply re-invokes election process    
def election_timeout(signum, frame):
    global ID, TERM
    LOG[TERM].append("Election timeout... invoking re-election on replica %d" % ID )
    election( timeout = True) ##commented out for now so we do not enter infinite elections

#  election():
#       prints message if initializing DB, otherwise called from timeout
#       updates dispatcher state to candidate, increments term, sets election timer
#       and sends "vote_4_me" requests to all clients
def election(timeout):
    print("invoking election...")
    global PENDING_ELECTION, TERM, STATE, LOG, VOTES, ID
    if not timeout:
        LOG[TERM].append("FOLLOWER: Initializing first election... Switching status to candidate...")
    else:
        TERM += 1
        LOG[TERM] = []
        LOG[TERM].append("FOLLOWER: Timeout occurred... Switching status to candidate for term %d" % TERM)

    PENDING_ELECTION = True
    STATE = "C"
    set_timer(election_timeout, reset = True) ##set timer...
    VOTES = []
    return
    
#  invoke_election():
#       heartbeat timeout occurred; print notification and start election process
def invoke_election(signum, frame): ##invoke election from heartbeat timeout (invoked from dispatcher)
    print("TIMEOUT: re-election invocation...")
    global STATE, LOG, ID, TERM
    if not STATE == "L": #if heartbeat timeout on non-leader replica...
        LOG[TERM].append( "Heartbeat timed out... invoking election on replica %d" %  ID )
        election(timeout = True)
    return


def handle_election_outcome(source_node, source_node_term): ##called by follower after election ends
    print("handling election outcome...")
    global PENDING_ELECTION, TERM, ELECTIONS, VOTED
    PENDING_ELECTION, VOTED = False, False
    ELECTIONS[source_node_term] = source_node
    TERM = source_node_term
    if TERM not in LOG.keys():
        LOG[TERM] = []
    return

def handle_heartbeats(source_node, source_node_term):
    global STATE, LOG, VOTED, RESET_TIMER, SET_TIMER, RECEIVED_FIRST_HEARTBEAT
    
    if STATE == "C": ##if candidate, this means election failed...
        LOG[TERM].append("CANDIDATE: election lost...")
        election_lost()
        
    if STATE == "L":    
        LOG[TERM].append("LEADER: new leader detected; switching status to follower")
        STATE = "F"
        
    if STATE == "F":
        if PENDING_ELECTION:
            handle_election_outcome(source_node, source_node_term)
            RESET_TIMER, SET_TIMER = False, True ##flag to start timer in next loop
            #set_timer( invoke_election, reset = False ) ##reset timer if new leader after election...
        
        RECEIVED_FIRST_HEARTBEAT = True
        RESET_TIMER, SET_TIMER = True, False  ##flag to reset timer in next loop
    return
    
######################## RPC REQUEST STATE & SIMPLE UPDATE STATE  ##################
#
#
#
####################################################################################
def get_server_id():
    return ID

def get_leader_id():
    global ELECTIONS
    return ELECTIONS[ max(ELECTIONS.keys())]

def get_term():
    global TERM, VOTED, PENDING_ELECTION
    return TERM

def get_state():
    global STATE
    return STATE

def update_log(log_str):
    global LOG
    LOG[TERM].append(log_str)
    return

def update_state(new_state):
    global STATE
    STATE = new_state
    return

def set_suspended():
    global SUSPENDED
    SUSPENDED = True
    return

def pending_value_lookup(key):
    global LOCAL_PENDING, TERM
    return LOCAL_PENDING[TERM][key]
######################## EVENT LOOP HELPERS, SEND RPC ##############################
#
#
#
####################################################################################
##handled when leader...
def send_heartbeats():
    print("sending heartbeats...")
    global SERVERS, TERM, ID, RECEIVED_FIRST_HEARTBEAT

    for server_key in SERVERS.keys():
        if server_key == ID:
            continue
        
        addr = SERVERS[server_key]
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.RaftStub(channel)
        term_ = TERM
        params = raft_pb2.heartbeat(source_node = ID, dst_node = server_key, term = term_)
        response = stub.heartbeatUpdate(params) ##response can be ignored...
    return

##handled when leader...
def send_appendEntry_requests():
    print("sending appendEntry requests...")
    global SERVERS, REMOTE_PENDING, LOCAL_PENDING, TERM, ID
    
    if TERM not in LOCAL_PENDING.keys():
        LOCAL_PENDING[TERM] = {}
    
    for commit_key, commit_val in LOCAL_PENDING[TERM].items():
        print("appendEntry:", commit_key, commit_val)
        
        if TERM not in REMOTE_PENDING.keys():
            REMOTE_PENDING[TERM] = {}
            
        if not commit_key in REMOTE_PENDING[TERM].keys():
            REMOTE_PENDING[TERM][commit_key] = []

        for server_key in SERVERS.keys():
            if server_key in REMOTE_PENDING[TERM][commit_key]: ##if server in list of servers that has already accepted key,val continue...
                continue
            
            if server_key == ID:
                REMOTE_PENDING[TERM][commit_key].append(ID)
                continue
            

            addr = SERVERS[server_key]
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.RaftStub(channel)
            
            term_ = TERM
            params = raft_pb2.appendEntry(source_node = ID, dst_node = server_key, term = term_,
                                          key = commit_key, value = commit_val)
            response = stub.AppendEntryRequest(params)
            print("ITEM PENDING?", response.outcome)
            if response.outcome:
                REMOTE_PENDING[TERM][commit_key].append(server_key)
    return

def update_votes(voter):
    global VOTES, PENDING_ELECTION, STATE, ELECTIONS
    quorum_reached = False
    if STATE == "C": ##if candidate and receives votes, update list of voters for candidate...
        if voter not in VOTES:
            VOTES.append(voter)
    
    print("CURRENT VOTES:", VOTES)

    if len(VOTES) >= QUORUM: ##if received a quorum of votes
        print("quorum reached. replica now leader.")
        PENDING_ELECTION = False #stop the election
        STATE = "L" #update state of dispatcher
        VOTES = [] ##re-initialize votes
        ELECTIONS[TERM] = ID
        stop_timer()
        quorum_reached = True
    return quorum_reached


def send_vote_4_mes():    
    print("sending vote_4_mes")
    global SERVERS, VOTES, ID, TERM, QUORUM, STATE
    for server_key in SERVERS.keys():
        
        if STATE != "C":
            return
        
        if server_key == ID:
            VOTES.append(ID)
            continue
        
        if server_key in VOTES:
            continue

        addr = SERVERS[server_key]
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.RaftStub(channel)

        term_ = TERM
        quorum_reached = False
        try:
            params = raft_pb2.vote_4_me(source_node = ID, dst_node = server_key, term = term_)
            print(params)
            response = stub.VoteRequest(params)
            if response.outcome:
                quorum_reached = update_votes(server_key)
        except:
            pass
                    
    if not quorum_reached:
        print("Did not receive enough votes & quorum not reached...")
        election_lost(leader_identified = False) #update state of dispatcher
    return

def send_commit_requests(commit_key):
    print("Sending commit requests...")
    global TERM, LOCAL_PENDING, REMOTE_PENDING, ID, SERVERS
    for server_key in SERVERS.keys():
        
        if server_key == ID:
            continue
        
        addr = SERVERS[server_key]
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.RaftStub(channel)
        term_ = TERM        
        params = raft_pb2.commitVal_request(source_node = ID, dst_node = server_key, term = term_,
                                            key = commit_key)
        response = stub.commitVal(params)
        return

def check_remote_pending():
    ##check REMOTE_PENDING... if any term has quorum of replicas pending on committing it,
    ##commit and send out commit requests.
    #remove it from LOCAL_PENDING & REMOTE_PENDING
    print("Checking remote pending...")
    global TERM, LOCAL_PENDING, REMOTE_PENDING, COMMITTED_DB
    
    with open('raft_db.pickle','rb') as handle:
            COMMITTED_DB = pickle.load( handle ) ##load DB
    
    if TERM not in LOCAL_PENDING.keys():
        LOCAL_PENDING[TERM] = {}
    
    for key, value in LOCAL_PENDING[TERM].items():
        if key in COMMITTED_DB:
            continue
        if TERM in REMOTE_PENDING.keys():
            if key in REMOTE_PENDING[TERM].keys():
                if len( REMOTE_PENDING[TERM][key] ) >= QUORUM:                        
                    send_commit_requests(key)
                    outcome = update_db(key, value)
    return

######################## SERVER INIT & EVENT LOOP ##################################
#
#
#
####################################################################################
def init(server_id):
    ###FETCH DB FROM PICKLE FILE OR INITIALIZE NEW DB...
    global SERVERS
    global STATE, ID, REPLICAS, QUORUM, TERM, SUSPENDED
    global LOG, ELECTIONS, SUSPENDED
    global PENDING_ELECTION, VOTES, REQUESTED_REPLICAS
    global LOCAL_PENDING, REMOTE_PENDING, VOTED ##handling pre-commit data
    global RECEIVED_FIRST_HEARTBEAT
    
    global SET_TIMER, RESET_TIMER 
    
    LOCAL_PENDING = {} ##store data until committed
    REMOTE_PENDING = {} ##leader track what is stored on other replicas

    STATE  = "F"
    ID = server_id
    REPLICAS = [i for i in SERVERS.keys() if i != ID ]
    QUORUM = math.floor( (len(REPLICAS)) / 2) + 1
    TERM = 0
    LOG = { 0 : []} #TERM : LOG ENTRIES
    ELECTIONS = {} ##no leaders yet... fill out as leaders get elected
    
    SET_TIMER, RESET_TIMER = False, False
    
    VOTED = False
    VOTES = []
    REQUESTED_REPLICAS = []
    PENDING_ELECTION = False
    RECEIVED_FIRST_HEARTBEAT = False
    
    SUSPENDED = False
    
    time.sleep(choice(range(0,10)))
    if not VOTED and not RECEIVED_FIRST_HEARTBEAT:
        set_timer(invoke_election, reset = False)
        election(timeout = False)
    print(STATE, TERM, PENDING_ELECTION, VOTED)    
    return
    

def event_loop(server):      
    global SUSPENDED, TERM, STATE, SET_TIMER, RESET_TIMER
    print("event loop...")
    print("STATE:", STATE)
    i = 0
    try:
        while True:
            i+=1
            
            if SUSPENDED: ##suspending to invoke leader re-election
                server.stop(0)
                time.sleep(5)
                server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
                raft_pb2_grpc.add_RaftServicer_to_server(RAFTServices(), server)
                server.add_insecure_port(SERVERS[ID])
                server.start()
                SUSPENDED = False

            if STATE == "F": ###if follower do nothing
                if SET_TIMER:
                    set_timer( invoke_election, reset = True )
                elif RESET_TIMER:
                    set_timer( invoke_election, reset = False )
                SET_TIMER, RESET_TIMER = False, False
            elif STATE == "C": ###if candidate send vote_4_mes to replicas that haven't voted for client yet
                send_vote_4_mes()
            elif STATE == "L": ##sending appendetnries and heartbeats
                print("leader jobs...")
                send_heartbeats() #sends heartbeats
                check_remote_pending() #checks if quorum of replicas have accepted appendEntry, if yes, send commit requests
                send_appendEntry_requests() #sends appendEntries to replicas not in REMOTE_PENDING, if any exist for this term
    
            time.sleep(3)
    except Exception as e:
        ##KEYBOARD INTERRUPT... PRINT LOG & SAVE TO OUTPUT...
        print('Exception occured')
        print(e)
        try:
            with open('raft_db.pickle','rb') as handle:
                COMMITTED_DB = pickle.load( handle ) ##load DB
        except:
            COMMITTED_DB = {}
            
        if TERM not in LOG.keys():
            LOG[TERM] = []
        if ELECTIONS != {}:
            LOG[TERM].append("[END OF SESSION] leader: %d\t term: %d" % (ELECTIONS[ max(ELECTIONS.keys())], TERM) )
        LOG[TERM].append( str("[END OF SESSION] elections: " + str(list(ELECTIONS.items()))) )
        LOG[TERM].append( str("[END OF SESSION] local pendingDB:" + str(list(LOCAL_PENDING.items()))) )
        LOG[TERM].append( str("[END OF SESSION] remote pendingDB:" + str(list(REMOTE_PENDING.items()))) )
        LOG[TERM].append( str("[END OF SESSION] commited_db:" + str(list(COMMITTED_DB.items()))) )
        
        ##turn off alarm...
        stop_timer()
        for item in LOG:
            print(item)
    return

########################################################################

class RAFTServices(raft_pb2_grpc.RaftServicer):
    ##HANDLE RPC CALLS....
    ##handles incoming RPC and passes to leader, follower, or candidate based on status...
    #def VoteRequest(source_node, dst_node, term):
    def VoteRequest(self, request, conext):
        print("receiving vote requests...")        
        replica_term = get_term()
        replica_state = get_state()
        
        if (request.term >= replica_term) and replica_state == "F":
            outcome = handle_vote_request(request.term)
            vote_4_me_reply = raft_pb2.voted_4_u(source_node = request.dst_node, dst_node = request.source_node,
                                                 term = replica_term, outcome = outcome)
        else:
            vote_4_me_reply = raft_pb2.voted_4_u(source_node = request.dst_node, dst_node = request.source_node,
                                                 term = replica_term, outcome = False)
        print(vote_4_me_reply)
        return vote_4_me_reply

    def AppendEntryRequest(self, request, context): #source_node, dst_node, term, key, value):
        print("receiving appendEntry request...")
        replica_term = get_term()
        replica_state = get_state()

        if request.term == replica_term and replica_state == "F":
            LOCAL_PENDING = handle_appendEntry(request.key, request.value)
            appendEntry_reply = raft_pb2.appendEntry_pending(source_node = request.dst_node, dst_node =  request.source_node,
                                                             term = replica_term, outcome = request.key in LOCAL_PENDING.keys())
        else:
            appendEntry_reply = raft_pb2.appendEntry_pending(source_node = request.dst_node, dst_node =  request.source_node,
                                                             term = replica_term, outcome = False)
        return appendEntry_reply
    
    def heartbeatUpdate(self, request, context): #source_node, dst_node, term):
        print("Receiving heartbeat...")
        replica_term = get_term()
        replica_state = get_state()
        
        if request.term > replica_term:
            if replica_state == "C": ##if candidate, this means election failed...
                log_str = str("CANDIDATE: Election for term %d lost... Returning to follower status" % replica_term )
                update_log(log_str)
                election_lost()
                
        if request.term >= replica_term:
            if replica_state == "L": ##notified of new leader
                log_str = str("LEADER: New leader detected... Updating to term %d and switching to follower status..." % replica_term)
                update_state("F")
                    
            handle_heartbeats(request.source_node, request.term)
        heartbeat_reply = raft_pb2.heartbeat_response(source_node = request.dst_node, dst_node = request.source_node,
                                                      term = replica_term)
        return heartbeat_reply
    
    def getVal(self, request, context):#key):
        print("Receiving getVal...")
        value = get_val(request.key)
        outcome = True if not value == "ERR" else False
        response = raft_pb2.getVal_response(value = value, outcome = outcome)
        return response
    
    def setVal(self, request, context):
        print("Receiving setVal...")
        set_bool, leader_id = set_val(request.key, request.value)
        if not set_bool:
            response = raft_pb2.setVal_response(outcome = set_bool)
        print(response)
        return response
    
    def commitVal(self, request, context): #source_node, dst_node, term, key):
        print("Receiving commitVal...")
        replica_term = get_term()
        outcome = update_db(request.key, 0)
        response = raft_pb2.commitVal_response(source_node = request.dst_node, dst_node = request.source_node,
                                               term = replica_term, outcome = outcome)
        return response

    def suspend(self, request, context):
        set_suspended() 
        response = raft_pb2.suspend_response(temp = 0)
        return response
    ##END HANDLE RPC CALLS....

def init_servers():
    global SERVERS
    SERVERS = {}

    with open("config.conf", "r") as f:
        for line in f:
            SERVERS[int(line.split(" ")[0])] = f'{line.split(" ")[1]}:{line.split(" ")[2].rstrip()}'

if __name__ == "__main__":
    global SERVERS
    print("init...")
    init_servers()
    print(SERVERS[int(sys.argv[1])])
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(RAFTServices(), server)
    server.add_insecure_port(SERVERS[int(sys.argv[1])])
    server.start()
    
    print('server started; sleeping before election... ')
    time.sleep(10)
    
    init(int(sys.argv[1]))
    
    event_loop(server)
