"""HITL API - FastAPI server for human approval workflow.

Endpoints:
  GET  /health                    - Health check
  GET  /approvals/pending         - Get pending approvals
  GET  /approvals/{id}            - Get specific approval
  POST /approvals/{id}/approve    - Approve a decision
  POST /approvals/{id}/reject     - Reject a decision
  GET  /approvals/history          - Get approval history
  GET  /stats                     - Get approval statistics
"""

import logging
from typing import Optional, Dict, List
from datetime import datetime

try:
    from fastapi import FastAPI, HTTPException, Body
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel
    import uvicorn
except ImportError:
    FastAPI = None
    HTTPException = None
    CORSMiddleware = None
    BaseModel = None
    uvicorn = None

from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import PolicyDecision, PolicyDecisionValue, PolicyStatus

from .approval_engine import ApprovalEngine, ApprovalStatus
from .rules import ApprovalAction

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Pydantic models
if BaseModel:
    class ApprovalRequest(BaseModel):
        """Approval request model."""
        approved_by: str
        note: Optional[str] = None
        override_weights: Optional[Dict[str, int]] = None

    class RejectionRequest(BaseModel):
        """Rejection request model."""
        rejected_by: str
        note: Optional[str] = None


class HITLConsumer(KafkaConsumerTemplate):
    """Consumer for policy decisions requiring approval."""

    def __init__(self, approval_engine: ApprovalEngine, producer: KafkaProducerTemplate):
        super().__init__(topics=["policy.decisions"], group_id="hitl_consumer")
        self.approval_engine = approval_engine
        self.producer = producer

    def process_message(self, topic: str, message: Dict) -> bool:
        """Process policy decision and determine if approval needed.
        
        Args:
            topic: Topic name
            message: Policy decision message
            
        Returns:
            True to commit, False to nack
        """
        try:
            decision_id = message.get("decision_id", "unknown")
            status = message.get("status", "unknown")
            
            # Only process PENDING decisions
            if status != PolicyStatus.PENDING.value:
                logger.debug(f"Skipping non-pending decision: {decision_id}")
                return True
            
            # Evaluate decision
            action, approval_id = self.approval_engine.process_decision(message)
            
            if action == ApprovalAction.AUTO_APPROVE:
                # Auto-approve and publish approved decision
                self._publish_approved_decision(message, "auto", "Auto-approved (low risk)")
                logger.info(f"Auto-approved decision: {decision_id}")
            
            elif action == ApprovalAction.AUTO_REJECT:
                # Auto-reject
                logger.info(f"Auto-rejected decision: {decision_id}")
            
            else:
                # Requires human approval
                logger.info(
                    f"Decision requires approval: {decision_id}, "
                    f"approval_id={approval_id}"
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing decision: {e}", exc_info=True)
            return False

    def _publish_approved_decision(
        self,
        decision: Dict,
        approved_by: str,
        note: str
    ) -> None:
        """Publish approved decision to execution topic.
        
        Args:
            decision: Original decision dict
            approved_by: Approver identifier
            note: Approval note
        """
        # Update decision status
        decision["status"] = PolicyStatus.APPROVED.value
        decision["metadata"]["approved_by"] = approved_by
        decision["metadata"]["approval_note"] = note
        decision["metadata"]["approved_at"] = datetime.now().isoformat()
        
        # Publish to execution topic
        event = PolicyDecision(
            key=decision.get("decision_id", "unknown"),
            value=PolicyDecisionValue(**decision)
        )
        
        self.producer.send("policy.approved", event)
        logger.info(f"Published approved decision: {decision.get('decision_id')}")


def create_app(approval_engine: ApprovalEngine, hitl_consumer: HITLConsumer) -> FastAPI:
    """Create FastAPI application.
    
    Args:
        approval_engine: ApprovalEngine instance
        hitl_consumer: HITLConsumer instance
        
    Returns:
        FastAPI app
    """
    if FastAPI is None:
        raise ImportError("FastAPI not installed. Install with: pip install fastapi uvicorn")
    
    app = FastAPI(
        title="HITL Approval API",
        description="Human-In-The-Loop approval system for policy decisions",
        version="1.0.0"
    )
    
    # CORS middleware - Allow all origins for development
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, specify allowed origins
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["*"],
        max_age=3600,
    )
    
    @app.get("/health")
    def health():
        """Health check endpoint."""
        return {
            "status": "healthy",
            "service": "hitl-api",
            "timestamp": datetime.now().isoformat()
        }
    
    @app.get("/approvals/pending")
    def get_pending_approvals():
        """Get all pending approval requests."""
        try:
            requests = approval_engine.get_pending_requests()
            return {
                "count": len(requests),
                "requests": requests
            }
        except Exception as e:
            logger.error(f"Error getting pending approvals: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/approvals/{approval_id}")
    def get_approval(approval_id: str):
        """Get specific approval request."""
        try:
            request = approval_engine.get_request(approval_id)
            if not request:
                raise HTTPException(status_code=404, detail="Approval request not found")
            return request
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error getting approval: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/approvals/{approval_id}/approve")
    def approve_decision(approval_id: str, request: ApprovalRequest):
        """Approve a pending decision."""
        try:
            # Get the approval request
            approval_req = approval_engine.get_request(approval_id)
            if not approval_req:
                raise HTTPException(status_code=404, detail="Approval request not found")
            
            if approval_req["status"] != ApprovalStatus.PENDING.value:
                raise HTTPException(
                    status_code=400,
                    detail=f"Request is not pending (status: {approval_req['status']})"
                )
            
            # Approve
            success = approval_engine.approve(
                approval_id,
                request.approved_by,
                request.note,
                request.override_weights
            )
            
            if not success:
                raise HTTPException(status_code=500, detail="Failed to approve request")
            
            # Publish approved decision
            decision = approval_req["decision"]
            if request.override_weights:
                decision["metadata"]["weights"] = request.override_weights
            
            hitl_consumer._publish_approved_decision(
                decision,
                request.approved_by,
                request.note or "Approved via HITL API"
            )
            
            return {
                "status": "approved",
                "approval_id": approval_id,
                "approved_by": request.approved_by
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error approving decision: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.post("/approvals/{approval_id}/reject")
    def reject_decision(approval_id: str, request: RejectionRequest):
        """Reject a pending decision."""
        try:
            # Get the approval request
            approval_req = approval_engine.get_request(approval_id)
            if not approval_req:
                raise HTTPException(status_code=404, detail="Approval request not found")
            
            if approval_req["status"] != ApprovalStatus.PENDING.value:
                raise HTTPException(
                    status_code=400,
                    detail=f"Request is not pending (status: {approval_req['status']})"
                )
            
            # Reject
            success = approval_engine.reject(
                approval_id,
                request.rejected_by,
                request.note
            )
            
            if not success:
                raise HTTPException(status_code=500, detail="Failed to reject request")
            
            return {
                "status": "rejected",
                "approval_id": approval_id,
                "rejected_by": request.rejected_by
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error rejecting decision: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/approvals/history")
    def get_approval_history(limit: int = 100):
        """Get approval history."""
        try:
            requests = approval_engine.get_completed_requests(limit)
            return {
                "count": len(requests),
                "requests": requests
            }
        except Exception as e:
            logger.error(f"Error getting approval history: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    @app.get("/stats")
    def get_stats():
        """Get approval statistics."""
        try:
            pending = approval_engine.get_pending_requests()
            completed = approval_engine.get_completed_requests()
            
            approved_count = sum(
                1 for req in completed
                if req["status"] in [ApprovalStatus.APPROVED.value, ApprovalStatus.AUTO_APPROVED.value]
            )
            rejected_count = sum(
                1 for req in completed
                if req["status"] == ApprovalStatus.REJECTED.value
            )
            
            return {
                "pending_count": len(pending),
                "approved_count": approved_count,
                "rejected_count": rejected_count,
                "total_processed": len(completed)
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    return app


def main():
    """Main entry point."""
    import os
    import threading
    
    port = int(os.getenv("HITL_PORT", "8080"))
    host = os.getenv("HITL_HOST", "0.0.0.0")
    
    logger.info("Starting HITL API Server")
    
    # Initialize components
    approval_engine = ApprovalEngine()
    producer = KafkaProducerTemplate()
    hitl_consumer = HITLConsumer(approval_engine, producer)
    
    # Create FastAPI app
    app = create_app(approval_engine, hitl_consumer)
    
    # Start Kafka consumer in background thread
    def run_consumer():
        try:
            hitl_consumer.start()
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
    
    consumer_thread = threading.Thread(target=run_consumer, daemon=True)
    consumer_thread.start()
    
    # Start FastAPI server
    logger.info(f"HITL API running on http://{host}:{port}")
    logger.info("Endpoints:")
    logger.info(f"  GET  http://{host}:{port}/health")
    logger.info(f"  GET  http://{host}:{port}/approvals/pending")
    logger.info(f"  POST http://{host}:{port}/approvals/{{id}}/approve")
    logger.info(f"  POST http://{host}:{port}/approvals/{{id}}/reject")
    
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
