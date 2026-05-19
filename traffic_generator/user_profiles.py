"""User Profiles - Different traffic generation patterns."""

import random
from enum import Enum
from dataclasses import dataclass
from typing import Callable


class TrafficPattern(Enum):
    """Traffic generation patterns."""
    CONSTANT = "constant"
    BURST = "burst"
    GRADUAL_INCREASE = "gradual_increase"
    SINE_WAVE = "sine_wave"
    RANDOM = "random"


@dataclass
class UserProfile:
    """User traffic profile."""
    name: str
    base_rps: float  # Base requests per second
    pattern: TrafficPattern
    duration_seconds: int = 60
    
    def get_rps(self, elapsed_seconds: int) -> float:
        """Get requests per second at given time.
        
        Args:
            elapsed_seconds: Seconds elapsed since start
            
        Returns:
            Requests per second at this time
        """
        if self.pattern == TrafficPattern.CONSTANT:
            return self.base_rps
        
        elif self.pattern == TrafficPattern.BURST:
            # Burst every 10 seconds
            if elapsed_seconds % 10 < 2:
                return self.base_rps * 5
            return self.base_rps
        
        elif self.pattern == TrafficPattern.GRADUAL_INCREASE:
            # Linear increase over duration
            progress = min(1.0, elapsed_seconds / self.duration_seconds)
            return self.base_rps * (1 + progress * 2)
        
        elif self.pattern == TrafficPattern.SINE_WAVE:
            # Sine wave pattern
            import math
            phase = (elapsed_seconds / 30) * 2 * math.pi
            return self.base_rps * (1 + 0.5 * math.sin(phase))
        
        elif self.pattern == TrafficPattern.RANDOM:
            # Random variation
            return self.base_rps * random.uniform(0.5, 2.0)
        
        return self.base_rps


# Predefined profiles
PROFILES = {
    "light": UserProfile(
        name="Light Load",
        base_rps=2.0,
        pattern=TrafficPattern.CONSTANT
    ),
    "moderate": UserProfile(
        name="Moderate Load",
        base_rps=10.0,
        pattern=TrafficPattern.CONSTANT
    ),
    "heavy": UserProfile(
        name="Heavy Load",
        base_rps=50.0,
        pattern=TrafficPattern.CONSTANT
    ),
    "burst": UserProfile(
        name="Burst Traffic",
        base_rps=5.0,
        pattern=TrafficPattern.BURST
    ),
    "ramp_up": UserProfile(
        name="Gradual Ramp Up",
        base_rps=5.0,
        pattern=TrafficPattern.GRADUAL_INCREASE,
        duration_seconds=120
    ),
    "sine_wave": UserProfile(
        name="Sine Wave",
        base_rps=10.0,
        pattern=TrafficPattern.SINE_WAVE
    ),
    "random": UserProfile(
        name="Random Load",
        base_rps=10.0,
        pattern=TrafficPattern.RANDOM
    ),
    "high_risk": UserProfile(
        name="High Risk Event",
        base_rps=200.0, # Massive RPS to trigger severe degradation and weight changes
        pattern=TrafficPattern.BURST
    ),
}
