package cmsc433.p4.messages;

import akka.actor.ActorRef;

public class WhoHasResourceResponseMsg {
	private final String resource_name;
	private final boolean result;
	private final ActorRef requestingUser;
	
	public WhoHasResourceResponseMsg (String resource_name, boolean result, ActorRef requestingUser) {
		this.resource_name = resource_name;
		this.result = result;
		this.requestingUser = requestingUser;
	}
	
	public WhoHasResourceResponseMsg (WhoHasResourceRequestMsg request, boolean result, ActorRef sender) {
		this.resource_name = request.getResourceName();
		this.result = result;
		this.requestingUser = request.getRequestingUser();
	}
	
	public String getResourceName () {
		return resource_name;
	}
	
	public boolean getResult () {
		return result;
	}
	
	public ActorRef getRequestingUser() {
		return requestingUser;
	}
	
	@Override public String toString () {
		return "I" + (result ? " have " : " do not have ") + resource_name;
	}
}
