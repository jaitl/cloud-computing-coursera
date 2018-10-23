/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	bool changed = false;

	/*
	* Step 3: Run the stabilization protocol IF REQUIRED
	*/
	if (!ring.empty()) {
		for (int i = 0; i < curMemList.size(); i++) {
			if (curMemList[i].getHashCode() != ring[i].getHashCode()) {
				changed = true;
				break;
			}
		}
	}

	ring = curMemList;

	if (changed) {
		stabilizationProtocol();
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	curMemList.emplace_back(Node(this->memberNode->addr));
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * Compute own Hash
 */
size_t MP2Node::myHash() {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(this->memberNode->addr.addr);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	handleAction(MessageType::CREATE, key, value);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	handleAction(MessageType::READ, key, "");
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	handleAction(MessageType::UPDATE, key, value);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	handleAction(MessageType::DELETE, key, "");
}

void MP2Node::handleAction(MessageType mType, string key, string value) {
	auto nodes = findNodes(key);
	int tId = createTransaction(mType, this->par->getcurrtime(), 3, key, value);

	for(auto node: nodes) {
		auto message = new Message(tId, memberNode->addr, mType, key, value);
		dispatchMessages(message, node.getAddress());
	}
}

void MP2Node::dispatchMessages(Message *message, Address *addr) {
	emulNet->ENsend(&memberNode->addr, addr, (char *)message, sizeof(Message));
}

void MP2Node::sendReply(Message *msg, bool res) {
	auto sendToAddr = msg->fromAddr;
	if (msg->type == MessageType::READ) {
		msg->type = MessageType::READREPLY;
	} else {
		msg->type = MessageType::REPLY;
	}
	msg->success = res;
	msg->fromAddr = memberNode->addr;
	dispatchMessages(msg, &sendToAddr);
}

int MP2Node::createTransaction(MessageType mType, int time, int rf, string key, string value) {
	auto id = g_transID++;
	auto t = new TransactionInfo {
		id: id,
		type: mType,
		createTime: time,
		replicationFactor: rf,
		replyCount: 0,
		key: key,
		value: value,
		logged: false,
		successCount: 0
	};
	transactionTable.emplace(id, t);
	return id;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, int tId) {
	auto res = this->ht->create(key, value);

	// hack for recover: not log recover messages
	if (tId != -1) {
		logOperation(MessageType::CREATE, false, res, tId, key, value);
	}

	return res;
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int tId) {
	auto value = this->ht->read(key);

	auto res = !value.empty();

	logOperation(MessageType::READ, false, res, tId, key, value);

	return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, int tId) {
	auto res = this->ht->update(key, value);

	logOperation(MessageType::UPDATE, false, res, tId, key, value);

	return res;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int tId) {
	auto res = this->ht->deleteKey(key);

	logOperation(MessageType::DELETE, false, res, tId, key, "");

	return res;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		auto msg = (Message *) data;

		/*
			* Handle the message types here
 		*/
		switch(msg->type) {
			case MessageType::CREATE: {
				auto res = createKeyValue(msg->key, msg->value, msg->transID);

				// hack for recover: not reply on recover messages
				if (msg->transID != -1) {
					sendReply(msg, res);
				}
			}
				break;
			case MessageType::READ: {
				auto res = readKey(msg->key, msg->transID);
				msg->value = res;
				sendReply(msg, !res.empty());
			}
				break;
			case MessageType::UPDATE: {
				auto res = updateKeyValue(msg->key, msg->value, msg->transID);
				sendReply(msg, res);
			}
				break;
			case MessageType::DELETE: {
				auto res = deletekey(msg->key, msg->transID);
				sendReply(msg, res);
			}
				break;
			case MessageType::REPLY: {
				auto trans = transactionTable[msg->transID];
				trans->replyCount++;
				if (msg->success) {
					trans->successCount++;
				}
			}
			case MessageType::READREPLY: {
				auto trans = transactionTable[msg->transID];
				trans->replyCount++;
				trans->value = msg->value;
				if (msg->success) {
					trans->successCount++;
				}
			}
				break;
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	checkTransaction();
}



void MP2Node::checkTransaction() {
	// check completed transaction
	for(auto t: transactionTable) {
		if (!t.second->logged && t.second->replyCount >= 2) {
			auto res = t.second->successCount == t.second->replyCount;
			logOperation(t.second->type, true, res, t.first, t.second->key, t.second->value);
			t.second->logged = true;
		}
	}

	// Check timeouts
	for(auto t: transactionTable) {
		if (!t.second->logged) {
			if (par->getcurrtime() - t.second->createTime > 10) {
				logOperation(t.second->type, true, false, t.first, t.second->key, t.second->value);
				t.second->logged = true;
			}
		}
	}
}

void MP2Node::logOperation(MessageType mType, bool isCoordinator, bool isSuccess, int transID, string key, string value) {
	switch (mType) {
		case CREATE: {
			if (isSuccess) {
				log->logCreateSuccess(&memberNode->addr, isCoordinator, transID, key, value);
			} else {
				log->logCreateFail(&memberNode->addr, isCoordinator, transID, key, value);
			}
		}
			break;
		case READ: {
			if (isSuccess) {
				log->logReadSuccess(&memberNode->addr, isCoordinator, transID, key, value);
			} else {
				log->logReadFail(&memberNode->addr, isCoordinator, transID, key);
			}
		}
			break;
		case UPDATE: {
			if (isSuccess) {
				log->logUpdateSuccess(&memberNode->addr, isCoordinator, transID, key, value);
			} else {
				log->logUpdateFail(&memberNode->addr, isCoordinator, transID, key, value);
			}
		}
			break;
		case DELETE: {
			if (isSuccess) {
				log->logDeleteSuccess(&memberNode->addr, isCoordinator, transID, key);
			} else {
				log->logDeleteFail(&memberNode->addr, isCoordinator, transID, key);
			}
		}
			break;
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	for(auto d: ht->hashTable) {
		auto nodes = findNodes(d.first);

		for(auto node: nodes) {
			auto message = new Message(-1, memberNode->addr, CREATE, d.first, d.second);
			dispatchMessages(message, node.getAddress());
		}
	}
}
