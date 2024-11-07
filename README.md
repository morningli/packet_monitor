# packet_monitor

## install lib

    sudo yum install -y libpcap
    sudo yum install -y libpcap-devel
    
## usage

print to shell
    
    ./packet_monitor -h <redis-host> -p <redis-port>
    
save to file
    
    ./packet_monitor -h <redis-host> -p <redis-port> -file out.txt

replay requests on other nodes
    
    ./packet_monitor -h <redis-host> -p <redis-port> -remote <remote-host>:<remote-port>    
