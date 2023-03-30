#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 19:40:06 2023

@author: miaweaver
"""
from random import choice
import signal
import math


class Dispatcher:
    def __init__(self, nodes):
        self.foo = 0
        self.curr_state = "F" #"L" or "C"
        self.id = 'FFFF' #some replica ID
        self.quorum = math.ceil(nodes/2) ##some subset of nodes
        self.term = 0
        
        self.event_loop()
        
    def listen(self):
        ##check if incoming RPC via pyRPC or socket...?
        return

    def send(self, msg):
        ##send outgoing RPC via pyRPC or socket
        return 
        
    def message_writer(self):
        ##package messages to send
        return
    
    def invoke_election(self, signum, frame):
        self.state = "C"
        print("timeout... in state C now")
        #self.reset_election_timeout()... redo the timeout for the election; if stalemate, another election
        ##write vote request messages
        ##send vote request messages to other candidates
        #vote_request = self.message_writer()
        return
    
    def handle_incoming_msgs(self, msg):
        ##check if msg needs to be handled by follower or leader process
            #if appendentry -> follower process IF replica state is follower
            #if client request -> leader process IF replica state is leader else pass to leader
            #if "vote for me" msg -> follower process IF replica state is follower
                #accept vote request if first received
        return

    def reset_heartbeat_timeout(self):
        signal.alarm(0) #stops current alarm
        election_timeout = choice(range(3, 5))
        signal.signal(signal.SIGALRM, self.invoke_election)
        signal.alarm(election_timeout)
        return
        

    def event_loop(self):      
        msgs_to_send  = []
        received_msgs = []
        
        election_timeout = choice(range(3, 5))
        signal.signal(signal.SIGALRM, self.invoke_election)
        signal.alarm(election_timeout)

        while 1:
            self.listen() ## check if any new rpc messages from clients or replicas
                
            if received_msgs:
                for msg in received_msgs:
                    if msg["type"] == "heartbeat": 
                        self.reset_heartbeat_timeout()
                    else:
                        self.handle_incoming_msgs(msg, msgs_to_send) ##updates the msgs to send

            if msgs_to_send:
                for msg in msgs_to_send:
                    self.send(msg)
                    
            
        return
        
if __name__ == "__main__":
    Dispatcher(7)