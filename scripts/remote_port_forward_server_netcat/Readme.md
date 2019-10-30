Developped by andysnake
Based on GNU netcat
because of different implementation of netcat on UNIX system, slight difference may be needed on script
	(e.g. listening socket on netcat on Debian based system -lp PORT on RedHat based system just -l PORT)
Because of eventual Double Nat, port forward may be impossible on home router (e.g. Fastweb :)
This script exploit an unnatted remote server (e.g. aws ec2 instance) that will expose desired port 
and will forward incoming tcp packet to home computer using a netcat relay, that will use an estambished connection initiated by home computer (that will add a rule in the not configurable nat)
So, basically for each port P desired to be exposed 2 netcat relay will be used
-1 netcat relay on remote server that forward pcks from desired to be exposed port P to relay port Pr (connected to home computer)
-1 netcat relay on home computer that forward pcks from remote server port Pr to local port P 
In the end Outside connection connect to Home Computer port P using remote server 
Enjoy
