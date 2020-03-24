Quick search for IP adrress in list within Kafka stream.
Tested on firewall logs and https://www.badips.com/get/list/any/2 bad ip list (around 430 000 IPs). Can process ~ 10kEPS as is (with print to terminal)
IPs stored in maps tree structures in memory. Search for one IP ~ 5 uSec

