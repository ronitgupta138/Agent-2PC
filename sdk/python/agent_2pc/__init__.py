"""
agent-2pc: Heterogeneous Two-Phase Commit Coordinator for AI Agent Workflows.
"""

from .client import Agent2PC, TxSession

__all__ = ["Agent2PC", "TxSession"]
__version__ = "2.0.0"
