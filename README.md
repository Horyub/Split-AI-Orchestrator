# Split-AI Orchestrator

## Introduction: thesis background and subsequent refactoring

**This project originated from a BSc thesis in Computer Engineering at the Università della Calabria (Unical), within the Department of Computer Engineering, Modeling, Electronics and Systems Engineering (DIMES). The code shared in this package is the result of a later refactor of the experimental implementation developed for that thesis.**

The original thesis is:

> Title (translated): **Execution of AI Models in the Core Network of 6G Networks: Implementation of an Orchestrator and Performance Evaluation**
> Author: Orazio Mattia Ruberto
> BSc in Computer Engineering, Università della Calabria
> Academic year 2024/2025
> Supervisor: Professor Antonio Iera
> Co-supervisor: Dr. Mattia Giovanni Spina

The code shared with this documentation results from later refactoring of that thesis implementation and preserves the main architecture and the separate processing scripts. The refactor includes structured messages, TCP assignment delivery with acknowledgements, request-based pairing of timing events, and revisions to resource monitoring. It retains the original approach of extending ComNetsEmu's classes to manage the service chain. The thesis and the refactored implementation represent different versions of the work; performance results reported in the thesis should be interpreted in the context of the implementation and experimental setup used at that time.

The following sections explain the implemented classes and their methods. The code excerpts are preserved from the supplied document.

## MyContainernet

`MyContainernet` extends the `Containernet` class provided by ComNetsEmu. Although the extension is small, inheritance makes it easier to introduce further changes later.

### Implementation

```python
class MyContainernet(Containernet):
    def __init__(self, **params):
        super().__init__(**params)
        self.monitorList = [] #list of DockerHost I want to monitor
        self.hostMap = {} # maps a host name with its respective instance

    # Overridden function used to identify hosts to be monitored
    def addDockerHost(self, name :str, monitor :bool, **params):
        host_name = name.upper()
        host = self.addHost(host_name, cls=DockerHost, **params)
        if monitor:
            self.monitorList.append(host)
        self.hostMap[host_name] = host
        return host
```

`super().__init__(**params)` initializes the base class while retaining the topology parameters. The two additional structures have different purposes: `hostMap` maps each host name to its object, while `monitorList` contains only the hosts selected for monitoring.

`addDockerHost()` overrides host creation. It normalizes the name to uppercase, delegates creation to `addHost(...)` with `DockerHost` as the class, updates the structures, and returns the created object. A host that is not monitored is still added to the registry: its availability for deployment does not depend on whether it has been selected for measurements.

## VNFManager: constructor and shared state

`VNFManager` inherits from `APPContainerManager`. Its constructor calls the base constructor and initializes the state required by subsequent operations.

### Initial manager state

```python
self.monitorList = net.monitorList
self.hostMap = net.hostMap
self.vnfList = []
self.priority_ip_mapping :dict = {}
self.end_to_end_delays = []
self.pending_times = {}  # request_id -> {send, receive, seen}
self.completed_requests = {} 
self.timing_timeout = 300.0  # Maximum time to retain unmatched UDP events.
self.state_lock = threading.Lock()
```

The assignments to `self.monitorList` and `self.hostMap` retain references to the network's structures rather than copying them. Hosts added later therefore also become visible to the manager.

- `vnfList` contains the VNFs created with `monitor=True`, rather than every application container.
- `priority_ip_mapping` associates each chain position with a list of addresses.
- `pending_times` stores timing events that have not yet been paired.
- `completed_requests` temporarily retains identifiers of completed requests.
- `end_to_end_delays` collects valid delay measurements.

The constructor also creates `state_lock` to protect the mapping and VNF list, and `e2e_lock` to protect timing measurement state.

### Threads created in the constructor

```python
self.resource_monitor = threading.Thread(target=self.start_monitoring, args=(1,))
self.resource_monitor.daemon = True
self.vm_power_cache = None

self.starting_handler=threading.Thread(target=self.get_starting_times)
self.starting_handler.daemon = True
self.starting_handler.start()

self.finishing_handler=threading.Thread(target=self.get_finishing_times)
self.finishing_handler.daemon = True
self.finishing_handler.start()
```

The resource monitor is prepared but not started. The two timing listeners start immediately.

## addVNF: creating and registering an application

```python
def addVNF(self, name: str, host: DockerHost, dimage: str,
           dcmd: str, priority :int = 0,
           docker_args: dict = None, wait: bool = True, monitor : bool = False) -> APPContainer:
    if host is None:
        raise ValueError(f"Missing host for VNF {name}")
    container = self.addContainer(name, host.name, dimage, dcmd, docker_args, wait)
    container.priority = priority
    container.placement_ip = host.IP()
    with self.state_lock:
        if priority >= 0:
            self.priority_ip_mapping.setdefault(str(priority), []).append(host.IP())
        if monitor:
            self.vnfList.append(container)
            self.jobs_to_monitor += 1
    return container
```

The method first checks that `host` is not `None`. Actual creation is delegated to `addContainer()`, inherited from ComNetsEmu's application manager. The arguments specify the container name, host name, image, command, Docker options, and whether to wait for creation.

The returned container receives two additional attributes: `priority`, which identifies its logical position in the chain, and `placement_ip`, which records where the instance was created. This recorded placement is also used during removal.

Inside the lock, `setdefault(..., [])` creates the address list if the position is not already present; `append()` adds the address of the new instance. Keys are strings because the mapping will be serialized as JSON. A negative position excludes the VNF from the chain, as happens for auxiliary services such as the IP dispatcher.

`vnfList` and `jobs_to_monitor` are updated only when `monitor` is true. They identify the subset of VNFs selected for monitoring.

## removeVNF: updating state after removal

```python
def removeVNF(self, vnf: APPContainer, wait: bool = False):
    if vnf is None:
        return
    vnf.dins.stop()
    self.removeContainer(vnf.name, wait)
    with self.state_lock:
        index = str(vnf.priority)
        addresses = self.priority_ip_mapping.get(index, [])
        if vnf.placement_ip in addresses:
            addresses.remove(vnf.placement_ip)
        if not addresses:
            self.priority_ip_mapping.pop(index, None)
        if vnf in self.vnfList:
            self.vnfList.remove(vnf)
            self.jobs_to_monitor -= 1
```

The method also accepts `None`, in which case it returns without performing any operation. Otherwise, it stops the Docker container through `dins` and delegates removal to `removeContainer()`.

The mapping update uses the address recorded in `placement_ip`. One occurrence is removed from the position's address list; if that list becomes empty, the key is removed as well. The monitored VNF count decreases only if the instance was actually present in `vnfList`.

These operations keep internal state consistent. Redistributing the mapping to the other nodes and managing requests that are still in progress require separate handling.

## create_split_chain: applying placement

This method calls an external optimizer to obtain the stage placement, then instantiates the stages:

```python
def create_split_chain(self, instance_name, gamma=0, epsilon=0.0):
    split_chain :dict = optimizer.execute(instance_name=instance_name, gamma=gamma, epsilon=epsilon)
    priority_cnt = 2
    for split in split_chain.keys():
        host_name = split_chain.get(split)
        self.addVNF(
            name=split,
            host= self.get_DockerHost_by_name(host_name),
            dimage="cupe_image",
            docker_args={
            "stdin_open": True,
            "volumes": {
                "shared_volume": {"bind": "/shared", "mode": "rw"}
            },
            "environment": {"SHARED_PATH": "/shared"}
            },
            dcmd=f"python3 /models/{split}.py",
            priority=priority_cnt,
            monitor=True
        )
        priority_cnt +=1
```

`optimizer.execute()` returns a dictionary mapping split names to host names. The priority counter starts at 2 because the preceding positions are reserved for the sender and the enhancer, or fixed first stage. These components and the receiver must be instantiated separately.

The excerpt above iterates over the optimizer dictionary in its insertion order. Stage priorities are assigned in that same order.

## From the internal mapping to TCP notification

### Generating the mapping

```python
def _generate_assignment_dict(self):
    with self.state_lock:
        return {key: values[-1] for key, values in self.priority_ip_mapping.items()
                if values}
```

The internal mapping stores lists, whereas the configuration sent to the nodes contains one address per position. `values[-1]` selects the most recently registered instance still present in the list.

This choice supports the intended migration scenario in which two instances of the same split may coexist: the old instance can finish previous computations while new requests are directed to the new instance. Completing that migration and managing requests in progress remain separate responsibilities; selecting the last address does not implement them by itself.

The lock protects dictionary construction. Communication takes place after this snapshot has been obtained.

### notify_containers

```python
def notify_containers(self):
    assignment = self._generate_assignment_dict()
    payload = AssignmentMessage(assignment).to_bytes()
    pending = set(assignment.values())
    dispatcher_addresses = {}
    for host in self.hostMap.values():
        ip = host.IP()
        if ip in pending:
            host.dins.reload()
            management_ip = host.dins.attrs["NetworkSettings"]["IPAddress"]
            if not management_ip:
                raise RuntimeError(
                    f"No Docker management IP for {host.name}"
                )
            dispatcher_addresses[ip] = management_ip
    missing_hosts = pending - set(dispatcher_addresses)
    if missing_hosts:
        raise RuntimeError(
            "No DockerHost registered for: "
            + ", ".join(sorted(missing_hosts))
        )
    for attempt in range(2):
        failed = set()
        for ip in sorted(pending):
            try:
                with socket.create_connection(
                    (dispatcher_addresses[ip], 26133), timeout=2.0
                ) as connection:
                    connection.sendall(payload)
                    connection.shutdown(socket.SHUT_WR)

                    if recv_until_eof(connection, limit=32) != b"ACK":
                        raise ValueError("Missing ACK")

            except (OSError, ValueError):
                failed.add(ip)
        pending = failed
        if not pending:
            return
        if attempt == 0:
            time.sleep(0.25)

    raise RuntimeError(
        "Assignment not acknowledged by: "
        + ", ".join(sorted(pending))
    )
```

`notify_containers()` distributes the Split-AI chain configuration to the dispatchers. The mapping is generated by `_generate_assignment_dict()`, validated through `AssignmentMessage`, and serialized with `to_bytes()`. The `pending` set collects destination host addresses and removes duplicates: a host running multiple stages receives one notification per attempt.

To reach the dispatchers, the method distinguishes emulated network addresses from Docker management addresses. The former are used by the splits to communicate through the topology. The latter allow the manager, running in the virtual machine, to contact the hosts through Docker's bridge network.

The method iterates over `hostMap`, identifies the hosts present in the mapping, and refreshes their Docker information with `host.dins.reload()`. It retrieves each management address from the `IPAddress` field of `NetworkSettings` and associates it with the emulated address in `dispatcher_addresses`. This association changes only the TCP connection destination: the configuration payload retains the emulated network addresses. An exception is raised if a registered host or its management address is missing.

A TCP connection is opened to each destination on port 26133, with a two-second timeout. `sendall()` sends the entire message. `shutdown(socket.SHUT_WR)` closes only the write side, indicating the end of the data to the dispatcher while leaving the connection available for reading the response. `recv_until_eof()` reconstructs the response until the dispatcher closes the connection, with a limit of 32 bytes. The response must match `b"ACK"` exactly.

Hosts for which communication fails are collected in `failed`. After a 0.25-second pause, the second attempt targets only those destinations. The method returns when every host has acknowledged the assignment. If any remain unconfirmed after both attempts, it raises `RuntimeError` listing their emulated addresses.

The ACK confirms that the dispatcher has finished writing the configuration to `ip.json`. It does not guarantee that the splits have already read the file or completed initialization.

## Receiving and pairing timing events

`get_starting_times()` and `get_finishing_times()` invoke the same listener with the expected event type. Their shared logic is:

### Shared timing listener

```python
def _listen_for_timing(self, port, event):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as rx_socket:
        rx_socket.bind(("", port))
        while True:
            raw = rx_socket.recv(8192)
            try:
                message = E2EMessage.from_bytes(raw)
                if message.event != event:
                    raise ValueError(f"Expected {event} event on {port}")
                self._record_timing(message)
            except (ValueError, TypeError) as error:
                print(f"Invalid timing message on {port}: {error}")
```

The listener receives a datagram, interprets it as an `E2EMessage`, and checks that `event` matches the channel on which it arrived. Invalid messages are reported without interrupting the loop. The listener delegates event pairing to `_record_timing()`.

### Pairing events

```python
def _record_timing(self, message):
    now = time.monotonic()
    with self.e2e_lock:
        # UDP can lose, duplicate or reorder messages, even across the two ports.
        self.pending_times = {key:value for key, value in self.pending_times.items()
                              if now - value['seen'] < self.timing_timeout}
        self.completed_requests = {key: value for key, value in self.completed_requests.items()
                                   if now - value < self.timing_timeout}
        if message.request_id in self.completed_requests:
            return
        times = self.pending_times.setdefault(message.request_id, {'seen': now})
        times.setdefault(message.event, message.timestamp)
        if 'send' in times and 'receive' in times:
            delay = times['receive'] - times['send']
            if delay >= 0:
                self.end_to_end_delays.append(delay)
            else:
                print(f"Negative E2E delay for {message.request_id}; check clocks")
            del self.pending_times[message.request_id]
            self.completed_requests[message.request_id] = now
```

`time.monotonic()` measures how long events should be retained; the message timestamps are used to calculate the delay. Both dictionaries are cleaned up whenever a new event arrives. There is no separate expiration thread.

The first check prevents an already completed request from being counted again. `setdefault()` creates the request state if necessary and records only the first timestamp of each event type. When both events are available, the delay is calculated as:

$$
T_{\mathrm{E2E}} = t_{\mathrm{receive}} - t_{\mathrm{send}}.
$$

A nonnegative value is added to the results list; otherwise, the inconsistency is reported. The pending state is removed and the identifier is recorded among completed requests. The lock makes these operations atomic with respect to the two listeners.

Pairing also works if the finish event arrives before the start event, but it cannot reconstruct lost UDP events.

## get_host_stats and start_monitoring

The call `vnf.dins.stats(stream=False)` retrieves container statistics. `get_host_stats()` derives CPU usage from the differences between current and previous counters:

### Calculating CPU percentage

```python
cpu_delta = current_cpu_usage - previous_cpu_usage
system_delta = current_system_usage - previous_system_usage

online_cpus = cpu_stats.get("online_cpus")

if not online_cpus:
    per_cpu_usage = (
        cpu_stats.get("cpu_usage", {}).get("percpu_usage", []) or []
    )
    online_cpus = max(1, len(per_cpu_usage))

cpu_percent = 0.0

if cpu_delta > 0 and system_delta > 0:
    cpu_percent = (
        (cpu_delta / system_delta)
        * float(online_cpus)
        * 100.0
    )
```

`cpu_delta` represents CPU time consumed by the container, while `system_delta` represents the overall CPU time recorded by the system. The number of logical CPUs expresses the percentage according to Docker's convention: 100% corresponds to one logical CPU.

For RAM, the method calculates `memory_working_set = max(0, memory_usage - cache)`, subtracting inactive file cache. The returned dictionary contains `cpu` and `memory` sections, which are consumed by the sampling loop:

### Resource monitoring loop

```python
def start_monitoring(self, sampling_time: float = 1):
    if sampling_time <= 0:
        raise ValueError("sampling_time must be positive")
    while True:
        try:
            host = self.get_DockerHost_by_name("UE")
            current_stats = self.get_host_stats(host)
            vm_power = self.get_vm_power_consumption("./", "vm_energy.json")
            cpu = current_stats['cpu']
            # Docker's 100% means ONE logical CPU, not the entire VM.
            cpu_fraction = cpu['usage_percentage'] / (100 * cpu['online_cpus'])
            estimated_power = None if vm_power is None else cpu_fraction * vm_power
            self.ue_power_consumptions.append(estimated_power)
            self.ue_ram_consumptions.append(current_stats['memory']['working_set_bytes'])
        except Exception as error:
            print(f"Resource monitoring failed: {error}")
        time.sleep(sampling_time) 
```

The loop explicitly selects the `UE` host. It reads its statistics, obtains the VM's power through `get_vm_power_consumption()`, and appends samples to the two lists. It does not automatically sample every host in `monitorList`.

Estimated power is calculated by multiplying the fraction of total CPU capacity used by the VM's power. This is a proportional allocation in watts, rather than a direct measurement of UE power or an energy value. `get_vm_power_consumption()` checks that the external value is finite and nonnegative. On error, it returns `None`, which is retained as a missing sample.

The wait is outside the `try` block, so it is also performed after an error, preventing a continuous loop without a pause. The interval must be positive. The thread is started explicitly with `resource_monitor.start()`, once per manager instance.

## Optional methods and experimental components

`get_prediction_times()` and `get_transfer_delays()` receive structured messages describing the execution time of an individual computation at each split and the time taken to transfer a computation's result to the following split.

Supporting functions include:

- `load_statistics()`, which reads numeric rows from a file and converts them into lists of values.
- `disable_IPV6()`, a function outside the classes that disables IPv6 on a node. IPv4 address assignment remains a separate topology configuration step.

Three methods remain experimental:

- `update_topology()` compares the current placement with a new assignment and can create a new instance after renaming the previous one. The code does not complete migration, management of requests in progress, or error recovery.
- `get_instantaneous_VNF_traffic()` is intended to read a transmitted-byte counter. It does not calculate throughput by itself.
- `monitor_vnf_throughput()` is intended to stop a VNF when throughput reaches zero. It remains an experimental component.

The application manager's `stop()` method remains inherited. No shutdown method has been added to stop all measurement threads in a coordinated way. Workload completion and result export must be managed by the program that uses the orchestrator.

## Message classes: messages.py

`messages.py` defines the format of messages exchanged between the manager, dispatchers, and Split-AI chain components. Centralizing these definitions allows the sender and receiver to use the same field names and validity checks.

All four classes use `@dataclass`: Python automatically generates the constructor from the declared fields. `__post_init__()` runs after object construction and checks the message content.

### AssignmentMessage: chain configuration

The `assignment` field is a dictionary mapping each stage's position to the IPv4 address of its host. Keys are numeric strings such as `"0"` or `"2"`. The class checks that the dictionary is nonempty, the keys have the expected format, and the addresses are valid IPv4 addresses.

The manager sends this message to the dispatchers, which extract the dictionary and write it to `ip.json`. The addresses in the message belong to the emulated network. Docker management addresses are used to establish the connection that delivers it.

### E2EMessage: end-to-end delay events

This class contains three fields: `request_id`, which identifies an inference request; `event`, which can be `"send"` or `"receive"`; and `timestamp`, which specifies the event time in seconds.

The manager pairs the events using `request_id` and calculates the delay by subtracting the send timestamp from the receive timestamp. The identifier makes it possible to recognize the same request even when notifications arrive in a different order. The class checks that the identifier is a nonnegative integer representable in 64 bits and that the timestamp is finite and nonnegative.

### PredictionTimeMessage: stage processing time

This class contains `job`, the stage index; `ip`, the address of its execution host; and `prediction_time`, the measured processing duration in seconds. `job` identifies the chain stage, whereas `request_id` in an end-to-end message identifies an individual inference request.

### TransferDelayMessage: transfer time between hosts

This class contains `job`, the stage index associated with the measurement; `ip_i` and `ip_j`, the addresses of the participating hosts; and `transfer_time`, the measured transfer time in seconds. As with the preceding message, the class validates the stage index, IPv4 addresses, and finite, nonnegative duration.

### Serialization and reconstruction

All classes expose the same two methods. `to_bytes()` converts the object into JSON and then into a byte sequence ready for transmission. `from_bytes()` decodes the JSON and reconstructs the object, running the validity checks again. These methods define the data representation; opening sockets, sending, and receiving remain the responsibility of the components using the messages.
