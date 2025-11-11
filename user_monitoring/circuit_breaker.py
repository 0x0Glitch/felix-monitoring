"""Circuit breaker pattern implementation for API resilience."""
import asyncio
import time
from enum import Enum
from typing import Optional, Callable, Any
import logging

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing recovery


class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures and reduce load on struggling services.
    
    States:
    - CLOSED: Normal operation, calls pass through
    - OPEN: Too many failures, calls rejected immediately
    - HALF_OPEN: Testing if service recovered, limited calls allowed
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = Exception,
        name: str = "CircuitBreaker"
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type to count as failure
            name: Name for logging
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name
        
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.state = CircuitState.CLOSED
        self._lock = asyncio.Lock()
        
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function through circuit breaker.
        
        Args:
            func: Async function to call
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            Exception: If circuit is open or function fails
        """
        async with self._lock:
            # Check circuit state
            if self.state == CircuitState.OPEN:
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    logger.info(f"{self.name}: Attempting recovery (moving to HALF_OPEN)")
                    self.state = CircuitState.HALF_OPEN
                else:
                    raise Exception(f"{self.name}: Circuit breaker is OPEN (service unavailable)")
        
        # Execute the function
        try:
            result = await func(*args, **kwargs)
            
            # Success - update state
            async with self._lock:
                if self.state == CircuitState.HALF_OPEN:
                    logger.info(f"{self.name}: Recovery successful (moving to CLOSED)")
                    self.state = CircuitState.CLOSED
                    self.failure_count = 0
                elif self.state == CircuitState.CLOSED:
                    # Reset failure count on success
                    self.failure_count = 0
                    
            return result
            
        except self.expected_exception as e:
            # Handle expected failure
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    if self.state != CircuitState.OPEN:
                        logger.error(
                            f"{self.name}: Too many failures ({self.failure_count}), "
                            f"opening circuit for {self.recovery_timeout}s"
                        )
                        self.state = CircuitState.OPEN
                else:
                    logger.warning(
                        f"{self.name}: Failure {self.failure_count}/{self.failure_threshold}"
                    )
            raise
    
    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self.state == CircuitState.OPEN
    
    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self.state == CircuitState.CLOSED
    
    def reset(self):
        """Manually reset the circuit breaker."""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        logger.info(f"{self.name}: Circuit breaker manually reset")
    
    def get_state(self) -> dict:
        """Get current state information."""
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "last_failure_time": self.last_failure_time,
            "is_available": self.state != CircuitState.OPEN
        }


class MultiCircuitBreaker:
    """Manage multiple circuit breakers for different endpoints."""
    
    def __init__(self, default_config: Optional[dict] = None):
        """
        Initialize multi-circuit breaker.
        
        Args:
            default_config: Default configuration for new breakers
        """
        self.breakers: dict[str, CircuitBreaker] = {}
        self.default_config = default_config or {
            "failure_threshold": 5,
            "recovery_timeout": 60.0
        }
    
    def get_breaker(self, name: str) -> CircuitBreaker:
        """Get or create a circuit breaker for the given name."""
        if name not in self.breakers:
            self.breakers[name] = CircuitBreaker(
                name=name,
                **self.default_config
            )
        return self.breakers[name]
    
    async def call(self, name: str, func: Callable, *args, **kwargs) -> Any:
        """Execute function through named circuit breaker."""
        breaker = self.get_breaker(name)
        return await breaker.call(func, *args, **kwargs)
    
    def get_status(self) -> dict:
        """Get status of all circuit breakers."""
        return {
            name: breaker.get_state()
            for name, breaker in self.breakers.items()
        }
    
    def reset_all(self):
        """Reset all circuit breakers."""
        for breaker in self.breakers.values():
            breaker.reset()
