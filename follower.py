#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 14:15:37 2023

@author: miaweaver
"""

class Follower:
    def __init__(self, replica_dispatcher, COMMITTED_DB, PENDING):
        self.db = COMMITTED_DB
        self.pending = PENDING
        
    def handle_appendEntry(self):
        ##TODO: add appentEntry to pendingDB & send msg of acceptance to leader
        return