
import socket
import time
import csv
import threading
import json
import sys
import subprocess
import os
import math
from messages import (AssignmentMessage, E2EMessage, PredictionTimeMessage,
                      TransferDelayMessage, recv_until_eof)

try:
    sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../split_ai_scheduling/build'))
    import run_and_plot as optimizer
except ImportError:
    optimizer = None
    print("Couldn't load optimizer")

from comnetsemu.net import APPContainerManager, Containernet
from comnetsemu.node import APPContainer, DockerHost

###################################################################################

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

###################################################################################

class VNFManager(APPContainerManager):
    """Manager for Virtualized Network Functions (specialized app containers)."""

    def __init__(self, net: MyContainernet):

        super().__init__(net)
        self.monitorList = net.monitorList
        self.hostMap = net.hostMap
        self.vnfList = []
        self.priority_ip_mapping :dict = {}
        self.end_to_end_delays = []
        self.pending_times = {}  # request_id -> {send, receive, seen}
        self.completed_requests = {}  # Brief duplicate suppression window.
        self.timing_timeout = 300.0  # Maximum time to retain unmatched UDP events.
        self.state_lock = threading.Lock()
        self.prediction_time_matrix = []
        self.jobs_to_monitor = 0

        #self.optimizer = optimizer

        self.ue_power_consumptions = []
        self.ue_ram_consumptions = []

        self.energy_consumptions = [[] for i in range(5)]
        self.ram_consumptions = [[] for i in range(5)]
        self.prediction_times = []
        self.time_delays = []

        self.e2e_lock = threading.Lock()

        #self.resource_monitor = threading.Thread(target=self.start_monitoring, args=(1,))
        #self.resource_monitor.daemon = True
        #self.vm_power_cache = None

        self.starting_handler=threading.Thread(target=self.get_starting_times)
        self.starting_handler.daemon = True
        self.starting_handler.start()

        self.finishing_handler=threading.Thread(target=self.get_finishing_times)
        self.finishing_handler.daemon = True
        self.finishing_handler.start()

    @classmethod
    def save_e2e_delays_to_csv(cls, data, data_name, filename, header):
        """Save the elapsed times list to a CSV file without clearing live measurements."""
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)  # Header row
            for value in list(data):
                writer.writerow([value])
        print(f"Saved {data_name} times to {filename}")

    @classmethod
    def save_data_to_csv(cls, data, data_name, filename, header):
        data = [list(column) for column in data]  
        with open(filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)  # Write header row

            if not data:
                print(f"Saved {data_name} to {filename} (empty)")
                return

            max_len = max(len(col) for col in data if hasattr(col, '__len__'))

            for row_idx in range(max_len):
                row = []
                for col in data:
                    # Add value if it exists at this index, otherwise empty string
                    if hasattr(col, '__getitem__') and row_idx < len(col):
                        row.append(col[row_idx])
                    else:
                        row.append('')
                writer.writerow(row)

        print(f"Saved {data_name} to {filename}")

    def save_prediction_times_to_csv(self, filename='prediction_times.csv'):
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['job', 'machine', 'prediction_time'])
            writer.writerows(list(self.prediction_times))

    def save_transfer_delays_to_csv(self, filename='transfer_delays.csv'):
        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['job', 'machine_i', 'machine_j', 'transfer_delay'])
            writer.writerows(list(self.time_delays))


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
                

    def get_host_stats(self, vnf):
        stats :dict = {}
        #raw_stats :dict = vnf.getCurrentStats() # returns a dictionary with the container stats

        raw_stats :dict = vnf.dins.stats(stream=False)

        cpu_stats = raw_stats.get("cpu_stats", {})
        previous_cpu_stats = raw_stats.get("precpu_stats", {})

        current_cpu_usage = (
            cpu_stats.get("cpu_usage", {}).get("total_usage", 0) or 0
        )
        previous_cpu_usage = (
            previous_cpu_stats.get("cpu_usage", {}).get("total_usage", 0) or 0
        )
        current_system_usage = cpu_stats.get("system_cpu_usage", 0) or 0
        previous_system_usage = (
            previous_cpu_stats.get("system_cpu_usage", 0) or 0
        )

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

        memory_stats = raw_stats.get("memory_stats", {})
        memory_usage = int(memory_stats.get("usage", 0) or 0)
        memory_limit = int(memory_stats.get("limit", 0) or 0)

        memory_details = memory_stats.get("stats", {}) or {}
        cache = int(
            memory_details.get(
                "total_inactive_file",
                memory_details.get("inactive_file", 0),
            )
            or 0
        )
        memory_working_set = max(0, memory_usage - cache)

        memory_percent = (
            (memory_working_set / memory_limit) * 100.0
            if memory_limit > 0
            else 0.0
        )

        return {
            "cpu": {
                "total_usage_nanoseconds": int(current_cpu_usage),
                "usage_percentage": cpu_percent,
                "online_cpus": online_cpus,
            },
            "memory": {
                "usage_bytes": memory_usage,
                "working_set_bytes": memory_working_set,
                "limit_bytes": memory_limit,
                "usage_in_mb": round(
                    memory_working_set / (1024 * 1024),
                    2,
                ),
                "usage_percentage": memory_percent,
            },
        }



    def get_vm_power_consumption(self, PATH: str, FILE: str):
        try:
            with open(os.path.join(PATH, FILE)) as source:
                power = float(json.load(source)["power_watts"])
            if not math.isfinite(power) or power < 0:
                raise ValueError("power_watts must be finite and nonnegative")
            self.vm_power_cache = power
            return power
        except (OSError, ValueError, TypeError, KeyError) as error:
            print(f"Cannot read VM power: {error}")
            return None

        

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

    def _generate_assignment_dict(self):
        with self.state_lock:
            return {key: values[-1] for key, values in self.priority_ip_mapping.items()
                    if values}


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



    def _record_timing(self, message):
        now = time.monotonic()
        with self.e2e_lock:
            # UDP can lose, duplicate or reorder messages, even across the two ports.
            self.pending_times = {key: value for key, value in self.pending_times.items()
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


    def get_starting_times(self):
        self._listen_for_timing(25956, 'send')


    def get_finishing_times(self):
        self._listen_for_timing(25957, 'receive')


    def get_machine_from_ip(self, ip):
        for index, host in enumerate(self.monitorList):
            try:
                intf = host.intf()
                if intf is not None and intf.IP() == ip:
                    return index
            except Exception:
                continue
        return -1
    

    def get_vnf_by_name(self, name :str):
        for vnf in self.vnfList:
            if vnf.name == name:
                return vnf
        return None


    def get_DockerHost_by_name(self, name :str):
        return self.hostMap.get(name.upper())

###############################################################################################################################################################

    # EXPERIMENTAL: migration/draining remains unimplemented; not used by this refactor.

    def update_topology(self, new_assignment :dict):
        # new_assignment == output dell'ottimizzatore come dizionario con chiave = split_i e valore l'host_j
        for vnf_name in new_assignment.keys():
            vnf = self.get_vnf_by_name(vnf_name)
            new_host = new_assignment[vnf_name]
            if vnf is None:
                raise ValueError(f"Unknown VNF: {vnf_name}")
            old_host = vnf.host if isinstance(vnf.host, str) else vnf.host.name
            new_host = new_host.upper()
            if self.get_DockerHost_by_name(new_host) is None:
                raise ValueError(f"Unknown host: {new_host}")
            if old_host != new_host:
                try:
                    subprocess.run(
                    ['docker', 'rename', f'{vnf_name}', f'{vnf_name}_old'],
                    check=True,
                    capture_output=True,
                    text=True
                    )
                    print("Renamed successfully")
                except subprocess.CalledProcessError as e:
                    print(f"Error: {e.stderr}")

                self.addVNF(name=vnf_name, host=self.hostMap.get(new_host),
                            dimage="cupe_image", dcmd=vnf.dcmd, priority=vnf.priority,
                            docker_args=vnf.docker_args, monitor=True)
                '''
                elimino subito la vecchia istanza
                o
                monitoro il suo throughput?
                '''

    def get_prediction_times(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as rx_socket:
            rx_socket.bind(("", 25954))
            while True:
                raw = rx_socket.recv(8192)
                try:
                    msg = PredictionTimeMessage.from_bytes(raw)
                    machine = self.get_machine_from_ip(msg.ip)
                    if machine >= 0:
                        self.prediction_times.append([msg.job, machine, msg.prediction_time])
                except (ValueError, TypeError, OSError) as error:
                    print(f"Invalid prediction message: {error}")

    def get_transfer_delays(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as rx_socket:
            rx_socket.bind(("", 25955))
            while True:
                raw = rx_socket.recv(8192)
                try:
                    msg = TransferDelayMessage.from_bytes(raw)
                    machine_i = self.get_machine_from_ip(msg.ip_i)
                    machine_j = self.get_machine_from_ip(msg.ip_j)
                    if machine_i >= 0 and machine_j >= 0 and machine_i != machine_j:
                        self.time_delays.append([msg.job, machine_i, machine_j, msg.transfer_time])
                except (ValueError, TypeError, OSError) as error:
                    print(f"Invalid transfer message: {error}")

    def load_statistics(self, file_path):
        res = []
        with open(file=file_path, mode="r", newline="\n") as f:
            for line in f:
                res.append([float(x) for x in line.replace(",", " ").split()])
        return res

    def get_instantaneous_VNF_traffic(self, vnf:APPContainer, target_network :str):
        #Non funziona nell'ambiente considerato
        d = {}
        raw_stats :dict = vnf.dins.stats(decode=False, stream=False)
        networks = raw_stats.get('networks', {})
        net = networks.get(target_network, {})
        return net.get('tx_bytes', -1)

    def monitor_vnf_throughput(self):
        rx_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        rx_socket.bind(("", 26134))
        print("Successfully binded on port 26134")
        while True:
            try:
                split_name = rx_socket.recv(8192).decode()
                vnf = self.get_vnf_by_name(split_name)
                if vnf is None:
                    continue
                vnf.dins.kill()
                print(f"VNF {vnf.name} successfully killed")
            except Exception as e:
                print(f"Error: {e}")

###############################################################################################

def disable_IPV6(node):
    node.cmd("sysctl -w net.ipv6.conf.all.disable_ipv6=1")
    node.cmd("sysctl -w net.ipv6.conf.default.disable_ipv6=1")
    node.cmd("sysctl -w net.ipv6.conf.lo.disable_ipv6=1")
