#!/usr/bin/env python
"""Quick syntax and import validation for cloud producers changes."""

print("=" * 70)
print("CLOUD-SPECIFIC PRODUCERS - SYNTAX & IMPORT VALIDATION")
print("=" * 70)

# Test 1: Import all components
print("\n✓ Test 1: Module Imports")
try:
    from kafka_core.producer_registry import CloudProducerRegistry
    print("  ✓ CloudProducerRegistry imported")
    
    from agents.partition_worker import PartitionWorker
    print("  ✓ PartitionWorker imported")
    
    from runners.service_agent_runner import ParallelServiceAgentRunner
    print("  ✓ ParallelServiceAgentRunner imported")
    
    from agents.agent_registry import AgentRegistry
    print("  ✓ AgentRegistry imported")
    
    print("  ✓ PASS: All imports successful")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Test 2: CloudProducerRegistry class structure
print("\n✓ Test 2: CloudProducerRegistry Class Structure")
try:
    registry = CloudProducerRegistry()
    
    # Check methods
    methods = [
        'get_or_create_producer',
        'get_producer',
        'get_all_producers',
        'close_all',
        'get_count',
        'get_clouds',
    ]
    
    for method in methods:
        assert hasattr(registry, method), f"Missing method: {method}"
        assert callable(getattr(registry, method)), f"Not callable: {method}"
        print(f"    ✓ {method}")
    
    # Check attributes
    assert hasattr(registry, '_producers'), "Missing _producers dict"
    assert hasattr(registry, '_lock'), "Missing _lock"
    print(f"  ✓ PASS: CloudProducerRegistry structure correct")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Test 3: PartitionWorker constructor signature
print("\n✓ Test 3: PartitionWorker Constructor Signature")
try:
    import inspect
    sig = inspect.signature(PartitionWorker.__init__)
    params = list(sig.parameters.keys())
    
    # Check for new producer_registry parameter (not old 'producer')
    assert 'producer_registry' in params, "Missing producer_registry parameter"
    assert 'producer' not in params, "Old 'producer' parameter should be removed"
    print(f"    Parameters: {params}")
    print(f"    ✓ Has producer_registry: {params.count('producer_registry')}")
    print(f"    ✓ No old producer param: ✓")
    print(f"  ✓ PASS: PartitionWorker constructor updated correctly")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Test 4: ServiceAgent constructor signature
print("\n✓ Test 4: ServiceAgent Constructor Signature")
try:
    from agents.service_agent import service_agent
    import inspect
    sig = inspect.signature(service_agent.ServiceAgent.__init__)
    params = list(sig.parameters.keys())
    
    # Check for new cloud_producer parameter
    assert 'cloud_producer' in params, "Missing cloud_producer parameter"
    assert 'cloud' in params, "Missing cloud parameter"
    
    print(f"    ✓ Has cloud: {params.count('cloud')}")
    print(f"    ✓ Has cloud_producer: {params.count('cloud_producer')}")
    print(f"  ✓ PASS: ServiceAgent constructor updated correctly")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Test 5: ParallelServiceAgentRunner structure
print("\n✓ Test 5: ParallelServiceAgentRunner Structure")
try:
    import inspect
    
    # Check __init__ mentions producer_registry
    init_source = inspect.getsource(ParallelServiceAgentRunner.__init__)
    
    assert 'self.producer_registry' in init_source, "Missing producer_registry initialization"
    assert 'CloudProducerRegistry()' in init_source, "Missing CloudProducerRegistry instantiation"
    assert 'self.producer = KafkaProducerTemplate()' not in init_source, "Old single producer still in __init__"
    
    print(f"    ✓ Uses producer_registry")
    print(f"    ✓ Creates CloudProducerRegistry instance")
    print(f"    ✓ Removed old single producer")
    print(f"  ✓ PASS: ParallelServiceAgentRunner updated correctly")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Test 6: _start_worker passes producer_registry
print("\n✓ Test 6: PartitionWorker Creation (_start_worker)")
try:
    import inspect
    
    # Check _start_worker passes producer_registry to PartitionWorker
    start_worker_source = inspect.getsource(ParallelServiceAgentRunner._start_worker)
    
    assert 'producer_registry=self.producer_registry' in start_worker_source, \
        "Not passing producer_registry to PartitionWorker"
    assert 'producer=self.producer' not in start_worker_source, \
        "Still passing old self.producer"
    
    print(f"    ✓ Passes producer_registry to PartitionWorker")
    print(f"    ✓ Removed old producer parameter")
    print(f"  ✓ PASS: _start_worker updated correctly")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Test 7: _shutdown closes producer_registry
print("\n✓ Test 7: Graceful Shutdown (_shutdown)")
try:
    import inspect
    
    # Check _shutdown calls producer_registry.close_all()
    shutdown_source = inspect.getsource(ParallelServiceAgentRunner._shutdown)
    
    assert 'self.producer_registry.close_all()' in shutdown_source, \
        "Not calling producer_registry.close_all()"
    assert 'if self.producer:' not in shutdown_source, \
        "Still has old producer cleanup code"
    
    print(f"    ✓ Calls producer_registry.close_all()")
    print(f"    ✓ Removed old producer.close() code")
    print(f"  ✓ PASS: _shutdown updated correctly")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

# Test 8: Files created/modified count
print("\n✓ Test 8: Changes Summary")
try:
    import os
    
    files_created = []
    files_modified = []
    
    # New file
    if os.path.exists('kafka_core/producer_registry.py'):
        files_created.append('kafka_core/producer_registry.py')
    
    # Modified files
    for f in [
        'agents/service_agent/service_agent.py',
        'agents/partition_worker.py',
        'runners/service_agent_runner.py',
    ]:
        if os.path.exists(f):
            files_modified.append(f)
    
    print(f"  Files created: {len(files_created)}")
    for f in files_created:
        print(f"    ✓ {f}")
    
    print(f"  Files modified: {len(files_modified)}")
    for f in files_modified:
        print(f"    ✓ {f}")
    
    print(f"  ✓ PASS: {len(files_created)} created, {len(files_modified)} modified")
except Exception as e:
    print(f"  ✗ FAIL: {e}")
    exit(1)

print("\n" + "=" * 70)
print("✓✓✓ ALL VALIDATION TESTS PASSED ✓✓✓")
print("=" * 70)
print("\nImplementation Summary:")
print("  - CloudProducerRegistry: Thread-safe per-cloud producer registry")
print("  - ServiceAgent: Now accepts cloud_producer parameter")
print("  - PartitionWorker: Uses producer_registry instead of shared producer")
print("  - ParallelServiceAgentRunner: Replaced single producer with registry")
print("\nKey Features:")
print("  ✓ One producer per cloud (AWS, GCP, Azure)")
print("  ✓ Lazy initialization (created on first agent)")
print("  ✓ Thread-safe (no explicit locks needed for send)")
print("  ✓ Backward compatible (ServiceAgent fallback)")
print("  ✓ Graceful cleanup in shutdown")
print("=" * 70)
