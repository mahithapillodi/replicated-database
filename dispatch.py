#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 19:40:06 2023

@author: miaweaver
"""

from leader import Leader
from follower import Follower
from candidate import Candidate

from random import choice
import signal
import math
import time

COMMITTED_DB = {"temp" : 0, "temp2" : 1}
PENDING = {}
##checking the index of requests... if already accepted val of higher index, reject


###DISPATCHER -- should be replicated and ran on all nodes. When ran, dispatcher also initializes candidate,
#                follower, and leader processes; these processes are only available if the replica state matches.
#               
#JOBS -- listens for and sends out RPC. Parses messages and passes them to appropriate processes (based on incoming
#        RPC and replica role). Packages responses into messages, to invoke proper processes on other replicas. Also
#        responsible for maintaining state (# nodes, # nodes for quorum, replica ID, pending DB, committed DB, term,
#        last election results, last entry)
# 
#https://stackoverflow.com/questions/3586106/perform-commands-over-ssh-with-python : code for ssh'ing into machine
#                           & running commands... could use this from host to initialize the dispatcher (which includes
#                           candidate, follower, and leader processes) across all 

##TODO: potentially separate storage of replica state from dispatcher processes
#       send msgs immediately as opposed to collecting msgs to send after parsing received msgs
#            should be fixed in write_msgs
#       received messages currently handled sequentially; should be handled concurrently
#       implement RPC; in the mean time, manually crafting and sending messages in the listen() function, which should
#            be where incoming RPC is detected

class Dispatcher:
    def __init__(self, replica_id, leader, nodes):
        self.curr_state = "F" if replica_id != leader else "L"
        self.id = replica_id
        self.replica_ids = [ i for i in range(nodes) if i != self.id ]
        self.quorum = math.floor(nodes/2) + 1 ##some subset of nodes
        self.term = 0
        
        self.leader_processes = Leader(self, COMMITTED_DB, PENDING)
        self.follower_processes = Follower(self, COMMITTED_DB, PENDING)
        self.candidate_processes = Candidate(self, COMMITTED_DB, PENDING)
        
        self.last_election = {self.term : leader} if leader else self.candidate_processes.election(timeout = False)
        self.last_entry = {}

        self.event_loop()
        
        
    def get_leader_id(self):
        return self.last_election[ max(self.last_election.keys()) ]

    ## RECEIVE AND SEND RPC
    # to be intialized later; in mean time, simulating msg passing...
    def listen(self, received_msgs):
        ##check if incoming RPC via pyRPC or socket...?
        ##NOTE: using manually crafted, temporary messages until RPC mechanism is implemented...
        m1 = { "type" : "heartbeat",
               "src"  : 0,
               "dst"  : 1,
               "cmd"  : None,
               "args" : [],
               "term" : 0
                }
        m2 = { "type" : "db_request",   ##req from client
               "src"  : 4,
               "dst"  : 1,
               "cmd"  : "get",
               "args" : ["temp"],
               "term" : 0
                }
        
        m3 = { "type" : "db_request",
               "src"  : 4,           ##reg from client
               "dst"  : 1,
               "cmd"  : "put",
               "args" : ["temp2", 4],
               "term" : 0} 

        #received_msgs.append(m1)
        #received_msgs.append(m2)
        received_msgs.append(m3)
        return

    def send(self, msg):
        ##send outgoing RPC via pyRPC or socket
        return 
    #''' end receive and send RPC '''

    #''' MESSAGE WRITER - for packaging outgoing rpc'''
    #can request msg be sent to ALL replicas, a specific replica, or to a subset of replicas;
    #depends on the type of the argument "dst"
    #cmd specifies if get or put (only relevant to appendEntries and db_requests)
    #args specify relevant arguments to put into neighbor replica processes
    def write_msgs(self, msg_type, dst, cmd, args, msgs_to_send):
        if dst == "all":
            for rep_id in self.replica_ids:
                temp_msg = { "type" : msg_type,
                             "src"  : self.id, ##replica ID
                             "dst"  : rep_id,
                             "cmd"  : cmd,
                             "args" : args,
                             "term" : self.term ##current term
                           }
                msgs_to_send.append(temp_msg)
                
        elif type(dst) is list:
            for rep_id in dst:
                temp_msg = { "type" : msg_type,
                             "src"  : self.id, ##replica ID
                             "dst"  : rep_id,
                             "cmd"  : cmd,
                             "args" : args,
                             "term" : self.term ##current term

                           }
                msgs_to_send.append(temp_msg)

        elif type(dst) is int and dst in self.replica_ids:
            temp_msg = { "type" : msg_type,
                         "src"  : self.id, ##replica ID
                         "dst"  : dst,
                         "cmd"  : cmd,
                         "args" : args,
                         "term" : self.term, ##current term
                       }
            msgs_to_send.append(temp_msg)
        return
    #''' end msg writer'''        

    # FUNCTION CALLED TO INITIALIZE/RESET HEARTBEAT / ELECTION TIMERS
    # func -- function to invoke if timeout
    #         in case of election timeout, re-invoke election; this case is called from candidate processes
    #         in case of heartbeat timeout,   invoke election; this case is calld from dispatch
    def set_timer(self, func, reset):
        if reset:
            signal.alarm(0) #stops current alarm
        heartbeat_timeout = choice(range(3, 5))
        signal.signal(signal.SIGALRM, func)
        signal.alarm(heartbeat_timeout)
        return
    #''' end set timer'''        


    def handle_heartbeats(self, msg):
        print("heartbeat detected... resetting timer")
        self.set_timer( self.candidate_processes.invoke_election, reset = True )
        if self.state == "C": ##if candidate, this means election failed...
            self.candidate_processes.election_lost()
        if self.state != "F": ##if for some reason not follower, update to follower...
            self.state = "F" 
        self.last_election[self.term] = msg["src"]  ##update last election results
        
    def handle_db_requests(self, msg, msgs_to_send):
        ##CHECK IF REPLICA IS LEADER OR FOLLOWER
        #db request sent from CLIENTS only... leader sends out appendentires
        #if client request -> leader process IF replica state is leader else return current leader

        if self.curr_state == "L":  #if leader, lookup get, or store put in pending DB and propagate
            PENDING, prop_pending, lookup_val = self.leader_processes.handle_db_reqeuests(msg)
            if prop_pending:
                print("updated pending db... sending to all replicas")
                self.write_msgs("db_request", "all", "put", prop_pending, msgs_to_send) ##prop to all replicas
                print(msgs_to_send)
            elif lookup_val:
                self.write_msgs("db_request_resp", msg["src"], None, [lookup_val], msgs_to_send)
                print("returned get request", lookup_val)
        elif self.curr_state == "F":  #if follower, return leader ID
            leader_id = self.last_election[ max(self.last_election.keys()) ]
            self.write_msgs("leader_id", msg["src"], None, [leader_id], msgs_to_send)
            print(msgs_to_send)
        return

    def handle_incoming_msgs(self, msg, msgs_to_send):
        if msg["type"] == "heartbeat":  ##if heartbeat, reset timer...
            print("heartbeat detected... resetting timer")
            self.set_timer( self.candidate_processes.invoke_election, reset = True )
        elif msg["type"] == "db_request": ##handle gets & puts
            self.handle_db_requests(msg, msgs_to_send)
        elif msg["type"] == "voted_4_u":
            self.candidate_processes.update_votes(msg["src"])
        elif msg["type"] == "appendEntry":
            self.follower_processes.handle_appendEntry(msg)
            
        ##TODO: handle vote_4_me msgs from candidates to followers and 
        #       appendEntry_pending msgs from followers to leaders
        return
        
    
    ##CHECK IF MSG IS VALID; DISCARD IF NOT...
    def check_valid_message(self, msg):
        if msg["dst"] != self.id: ##discard msg if not intended for replica
            return False
        if msg["term"] < self.term: ##discard msg if from previous term
            return False
        if msg["term"] > self.term: ##if observing a higher term
            self.term = msg["term"] #update term
        if msg["type"] == "appendEntry" and msg["src"] != self.get_leader_id() and self.state == "F":
            return False ## only accept appendentries from leader to follower
        return True
    # ''' end msg verifier '''


    ## EVENT_LOOP - constantly running, checking for new incoming RPC, 
    ## handling incoming RPC, and sending response RPC... invokes election if necessary via
    ## signal timer process
    def event_loop(self):      
        msgs_to_send  = [] ##store outbound messages
        received_msgs = [] ##store inbound messages to parse
        
        self.set_timer( self.candidate_processes.invoke_election, reset = False) ##initialize timer

        i = 0
        while i < 1: ## 1 = number of times to check for new RPC
            self.listen(received_msgs) ## check if any new rpc messages from clients or replicas
            if received_msgs: ## if yes...
                print("messages received...")
                for msg in received_msgs: ##TODO: HANDLE CONCURRENTLY...
                    print(msg["type"])
                    received_msgs.remove(msg)
                    if self.check_valid_message(msg): ##check if valid; if yes, handle msg and send appropriate response...
                        self.handle_incoming_msgs(msg, msgs_to_send) ##updates the msgs to send

            if msgs_to_send:
                for msg in msgs_to_send:
                    self.send(msg)
            i+=1
            print("local pendingDB", PENDING)
            
        signal.alarm(0) ##turn off alarm...
        return
            
if __name__ == "__main__":
    Dispatcher(1, None, 7)