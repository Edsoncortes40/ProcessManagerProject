package cmsc433.p4.messages;

import akka.actor.ActorRef;
import cmsc433.p4.enums.AccessRequestType;

public class WhoHasResourceRequestMsg {	
	private final String resource_name;
	//private final ActorRef requestingManager;
	private final ActorRef requestingUser;
	//private final AccessRequestType requestType;
	
	public WhoHasResourceRequestMsg (String resource, ActorRef requestingUser) {
		this.resource_name = resource;
		this.requestingUser = requestingUser;
		//this.requestType = requestType;
	}
	
	public String getResourceName () {
		return resource_name;
	}
	
	public ActorRef getRequestingUser() {
		return requestingUser;
	}
	
	@Override 
	public String toString () {
		return "Who has " + resource_name + "?";
	}
}
