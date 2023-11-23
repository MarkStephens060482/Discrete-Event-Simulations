Discrete Event Simulation in Julia: M/M/1 Queue System
Overview
This project demonstrates a Discrete Event Simulation (DES) implementation in Julia focusing on an M/M/1 queue system. The simulation models a factory production line where products move through a single machine with occasional breakdowns and repairs.

Features
M/M/1 Queue Model: The system follows the M/M/1 queuing model, representing a single-server queue with exponential arrival and service times.
Machine Breakdowns: Introduces machine breakdowns following a stochastic process and includes repair times before the machine resumes service.
Event-Driven Simulation: Utilizes discrete event simulation techniques where events trigger changes in the system state.
Metrics and Analysis: Captures and analyzes key performance metrics like throughput, waiting times, and machine utilization.
