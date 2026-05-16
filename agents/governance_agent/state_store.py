"""
Governance State Store: MongoDB Primary + In-Memory Fallback

Persists governance decisions (previous applied weights) for future KL computations.
Falls back to in-memory cache if MongoDB unavailable (non-fatal).
"""

import logging
import os
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Try to import pymongo; if unavailable, fallback is used
try:
    from pymongo import MongoClient
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False
    logger.warning("pymongo not installed; governance state will use in-memory fallback only")


class InMemoryStore:
    """Simple in-memory store for governance state."""

    def __init__(self):
        self.state = {
            "weights": None,
            "timestamp": None,
            "decision_id": None,
            "correlation_id": None,
        }
        logger.debug("InMemoryStore initialized")

    def get_previous_weights(self) -> Optional[Dict[str, float]]:
        """Get last applied weights or None if never applied."""
        return self.state.get("weights")

    def save_applied_weights(
        self,
        weights: Dict[str, float],
        decision_id: str,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Save applied weights to in-memory cache."""
        self.state["weights"] = weights
        self.state["decision_id"] = decision_id
        self.state["correlation_id"] = correlation_id
        self.state["timestamp"] = datetime.utcnow()
        logger.debug(f"InMemoryStore saved weights: decision_id={decision_id}, sum={sum(weights.values())}")


class MongoStore:
    """MongoDB-backed store for governance state with in-memory fallback."""

    def __init__(self, mongo_uri: str, db_name: str = "governance", collection_name: str = "state"):
        """
        Initialize MongoDB store.

        Args:
            mongo_uri: MongoDB connection string
            db_name: Database name
            collection_name: Collection name for state documents
        """
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = None
        self.collection = None
        self.fallback_store = InMemoryStore()
        self._failed_once = False  # Log connection failure only once

        self._connect()

    def _connect(self) -> None:
        """Attempt to connect to MongoDB."""
        try:
            self.client = MongoClient(
                self.mongo_uri,
                connectTimeoutMS=5000,
                serverSelectionTimeoutMS=5000,
                retryWrites=False,
            )
            # Verify connection
            self.client.server_info()
            self.collection = self.client[self.db_name][self.collection_name]
            logger.info(f"MongoDB connected: {self.mongo_uri} → db={self.db_name}, collection={self.collection_name}")
        except Exception as e:
            logger.warning(f"MongoDB connection failed: {e}. Using in-memory fallback.")
            self.client = None
            self.collection = None

    def get_previous_weights(self) -> Optional[Dict[str, float]]:
        """Get last applied weights from MongoDB or fallback."""
        if self.collection is None:
            return self.fallback_store.get_previous_weights()

        try:
            doc = self.collection.find_one(sort=[("timestamp", -1)])
            if doc:
                weights = doc.get("weights")
                logger.debug(f"MongoStore retrieved weights: decision_id={doc.get('decision_id')}")
                return weights
            return None
        except Exception as e:
            logger.error(f"MongoStore read error: {e}. Falling back to in-memory.", exc_info=False)
            return self.fallback_store.get_previous_weights()

    def save_applied_weights(
        self,
        weights: Dict[str, float],
        decision_id: str,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Save applied weights to MongoDB and fallback cache."""
        doc = {
            "decision_id": decision_id,
            "correlation_id": correlation_id,
            "weights": weights,
            "timestamp": datetime.utcnow(),
        }

        if self.collection is not None:
            try:
                result = self.collection.insert_one(doc)
                logger.debug(
                    f"MongoStore saved weights: decision_id={decision_id}, inserted_id={result.inserted_id}"
                )
            except Exception as e:
                logger.error(f"MongoStore write error: {e}. Falling back to in-memory.", exc_info=False)
                self.fallback_store.save_applied_weights(weights, decision_id, correlation_id)
        else:
            self.fallback_store.save_applied_weights(weights, decision_id, correlation_id)

    def close(self) -> None:
        """Close MongoDB connection."""
        if self.client is not None:
            try:
                self.client.close()
                logger.info("MongoDB connection closed")
            except Exception as e:
                logger.error(f"Error closing MongoDB: {e}")


class GovernanceStateStore:
    """Factory for creating appropriate state store based on configuration."""

    @staticmethod
    def create(use_mongo: bool = True) -> "Optional[MongoStore] | InMemoryStore":
        """
        Create store instance.

        Args:
            use_mongo: If True, try MongoDB; fall back if unavailable. If False, use in-memory only.

        Returns:
            MongoStore (with fallback) or InMemoryStore
        """
        if use_mongo and HAS_PYMONGO:
            mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
            return MongoStore(mongo_uri)
        else:
            if use_mongo:
                logger.warning("pymongo not available; requested MongoDB but using in-memory store")
            else:
                logger.info("Using in-memory governance state store")
            return InMemoryStore()
