"""
Quick validation script to verify all imports and basic instantiation.
Run this to ensure all components are properly integrated.
"""

import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def validate_imports():
    """Verify all new modules can be imported."""
    logger.info("✓ Validating imports...")
    
    try:
        from kafka_core.partition_queue_manager import PartitionQueueManager
        logger.info("  ✓ PartitionQueueManager")
        
        from agents.agent_registry import AgentRegistry
        logger.info("  ✓ AgentRegistry")
        
        from kafka_core.dispatcher import Dispatcher
        logger.info("  ✓ Dispatcher")
        
        from agents.partition_worker import PartitionWorker
        logger.info("  ✓ PartitionWorker")
        
        from kafka_core.utils import extract_key_parts
        logger.info("  ✓ kafka_core.utils")
        
        from config.agent_config_loader import AgentConfigLoader
        logger.info("  ✓ AgentConfigLoader")
        
        from runners.service_agent_runner import ParallelServiceAgentRunner
        logger.info("  ✓ ParallelServiceAgentRunner")
        
        return True
    except Exception as e:
        logger.error(f"✗ Import failed: {e}", exc_info=True)
        return False

def validate_basic_instantiation():
    """Verify basic instantiation of key components."""
    logger.info("\n✓ Validating basic instantiation...")
    
    try:
        from kafka_core.partition_queue_manager import PartitionQueueManager
        qm = PartitionQueueManager(default_maxsize=1000)
        logger.info("  ✓ PartitionQueueManager instantiated")
        
        from agents.agent_registry import AgentRegistry
        ar = AgentRegistry()
        logger.info("  ✓ AgentRegistry instantiated")
        
        from kafka_core.dispatcher import Dispatcher
        d = Dispatcher()
        logger.info("  ✓ Dispatcher instantiated")
        
        from kafka_core.utils import extract_key_parts
        service_id, cloud = extract_key_parts("service-api@aws")
        assert service_id == "service-api" and cloud == "aws"
        logger.info("  ✓ extract_key_parts working correctly")
        
        return True
    except Exception as e:
        logger.error(f"✗ Instantiation failed: {e}", exc_info=True)
        return False

def validate_config_loading():
    """Verify config file loading."""
    logger.info("\n✓ Validating config loading...")
    
    try:
        from config.agent_config_loader import AgentConfigLoader
        config = AgentConfigLoader.load_config("config/agents.yml")
        
        logger.info(f"  ✓ Config loaded successfully")
        logger.info(f"    - Group ID: {config['group_id']}")
        logger.info(f"    - Agents: {len(config['agents'])}")
        for agent in config['agents']:
            logger.info(f"      - {agent['service_id']}@{agent['cloud']}")
        
        return True
    except Exception as e:
        logger.error(f"✗ Config loading failed: {e}", exc_info=True)
        return False

def main():
    """Run all validation checks."""
    logger.info("=" * 60)
    logger.info("PHASES 1-6 IMPLEMENTATION VALIDATION")
    logger.info("=" * 60)
    
    results = []
    results.append(("Imports", validate_imports()))
    results.append(("Basic Instantiation", validate_basic_instantiation()))
    results.append(("Config Loading", validate_config_loading()))
    
    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"{status}: {name}")
        if not passed:
            all_passed = False
    
    logger.info("=" * 60)
    
    if all_passed:
        logger.info("\n✓✓✓ All validations passed! Ready for testing. ✓✓✓\n")
        return 0
    else:
        logger.error("\n✗✗✗ Some validations failed. See errors above. ✗✗✗\n")
        return 1

if __name__ == "__main__":
    sys.exit(main())
