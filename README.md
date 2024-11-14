# packet_monitor

## install lib

    sudo yum install -y libpcap
    sudo yum install -y libpcap-devel
    
## usage

print to shell
    
    ./packet_monitor -h <redis-host> -p <redis-port>
    
save to file
    
    ./packet_monitor -h <redis-host> -p <redis-port> -o file:out.txt

replay requests on other nodes
    
    ./packet_monitor -h <redis-host> -p <redis-port> -o single:<remote-host>:<remote-port>
    ./packet_monitor -h <redis-host> -p <redis-port> -o cluster:<remote-host>:<remote-port>,<remote-host>:<remote-port>    

print packets only

    ./packet_monitor -h <redis-host> -p <redis-port> -P raw

## build 

### centos 

    CGO_LDFLAGS=-lpcap go build