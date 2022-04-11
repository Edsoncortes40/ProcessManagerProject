package cmsc433.p4.actors;
import java.util.HashMap;
import java.util.LinkedList;
import cmsc433.p4.enums.*;
import cmsc433.p4.messages.*;
import cmsc433.p4.util.*;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.AbstractActor.Receive;
import akka.actor.AbstractActor;

public class ResourceManagerActor extends AbstractActor {
	
	private ActorRef logger;					// Actor to send logging messages to
	
	//variables
	LinkedList<ActorRef> resourceManagers;
	LinkedList<ActorRef> localUsers;
	HashMap<String, Resource> resourceNames;
	
	HashMap<Resource, LinkedList<ActorRef>> resourceQueues = new HashMap<Resource, LinkedList<ActorRef>>();
	HashMap<Resource, LinkedList<AccessRequestMsg>> queueMessages = new HashMap<Resource, LinkedList<AccessRequestMsg>>();
	
	HashMap<Resource, LinkedList<ActorRef>> readAccess = new HashMap<Resource, LinkedList<ActorRef>>();
	HashMap<Resource, LinkedList<ActorRef>> writeAccess = new HashMap<Resource, LinkedList<ActorRef>>();
	
	HashMap<Resource, Boolean> pendingDisable = new HashMap<Resource, Boolean>();
	HashMap<Resource, LinkedList<ActorRef>> awaitingDisable = new HashMap<Resource, LinkedList<ActorRef>>();
	HashMap<Resource, LinkedList<ManagementRequestMsg>> disableMessages = new HashMap<Resource, LinkedList<ManagementRequestMsg>>();
	
	HashMap<String, ActorRef> resourceMap = new HashMap<String, ActorRef>();
	HashMap<String, HashMap<ActorRef, Object>> remoteMessages = new HashMap<String, HashMap<ActorRef, Object>>();
	HashMap<String, Integer> resourceCounts = new HashMap<String, Integer>();
	
	
	/**
	 * Props structure-generator for this class.
	 * @return  Props structure
	 */
	static Props props (ActorRef logger) {
		return Props.create(ResourceManagerActor.class, logger);
	}
	
	/**
	 * Factory method for creating resource managers
	 * @param logger			Actor to send logging messages to
	 * @param system			Actor system in which manager will execute
	 * @return					Reference to new manager
	 */
	public static ActorRef makeResourceManager (ActorRef logger, ActorSystem system) {
		ActorRef newManager = system.actorOf(props(logger));
		return newManager;
	}
	
	/**
	 * Sends a message to the Logger Actor
	 * @param msg The message to be sent to the logger
	 */
	public void log (LogMsg msg) {
		logger.tell(msg, getSelf());
		//System.out.println(msg.toString());
	}
	
	/**
	 * Constructor
	 * 
	 * @param logger			Actor to send logging messages to
	 */
	private ResourceManagerActor(ActorRef logger) {
		super();
		this.logger = logger;
	}
	
	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(Object.class, this::onReceive)
				.build();
	}

	// You may want to add data structures for managing local resources and users, storing
	// remote managers, etc.
	//
	// REMEMBER:  YOU ARE NOT ALLOWED TO CREATE MUTABLE DATA STRUCTURES THAT ARE SHARED BY
	// MULTIPLE ACTORS!
	
	/* (non-Javadoc)
	 * 
	 * You must provide an implementation of the onReceive() method below.
	 * 
	 * @see akka.actor.AbstractActor#createReceive
	 */
	
	public void onReceive(Object msg) throws Exception {
		
		try {
			
		ActorRef sender = getSender();
		
		//==========INITIALIZATION==========
		if(msg instanceof AddRemoteManagersRequestMsg) 
		{
			resourceManagers = new LinkedList<ActorRef>(((AddRemoteManagersRequestMsg) msg).getManagerList());
			
			//respond to sender
			sender.tell(new AddRemoteManagersResponseMsg((AddRemoteManagersRequestMsg) msg), getSelf());
		}
		else if(msg instanceof AddLocalUsersRequestMsg) 
		{
			localUsers = new LinkedList<ActorRef>(((AddLocalUsersRequestMsg) msg).getLocalUsers());
			
			//respond to sender
			sender.tell(new AddLocalUsersResponseMsg((AddLocalUsersRequestMsg) msg), getSelf());
		}
		else if(msg instanceof AddInitialLocalResourcesRequestMsg)
		{
			LinkedList<Resource> resources = new LinkedList<Resource>(((AddInitialLocalResourcesRequestMsg) msg).getLocalResources());
			resourceNames = new HashMap<String, Resource>();
			
			//enable all resources
			for(int i = 0; i < resources.size(); i++) {
				resources.get(i).enable();
				resourceNames.put(resources.get(i).getName(), resources.get(i));
				resourceQueues.put(resources.get(i), new LinkedList<ActorRef>());
				queueMessages.put(resources.get(i), new LinkedList<AccessRequestMsg>());
				writeAccess.put(resources.get(i), new LinkedList<ActorRef>());
				readAccess.put(resources.get(i), new LinkedList<ActorRef>());
				pendingDisable.put(resources.get(i), false);
				awaitingDisable.put(resources.get(i), new LinkedList<ActorRef>());
				resourceMap.put(resources.get(i).getName(), getSelf());
				log(LogMsg.makeLocalResourceCreatedLogMsg(getSelf(), resources.get(i).getName()));
			}
			
			//respond to sender
			sender.tell(new AddInitialLocalResourcesResponseMsg((AddInitialLocalResourcesRequestMsg) msg), getSelf());
		}
		
		//==========LOCAL REQUEST PROCESSING==========
		else if(msg instanceof AccessRequestMsg) 
		{
			AccessRequestType request = ((AccessRequestMsg) msg).getAccessRequest().getType();
			String resourceName = ((AccessRequestMsg) msg).getAccessRequest().getResourceName();
			ActorRef requestingUser = ((AccessRequestMsg) msg).getReplyTo();
			log(LogMsg.makeAccessRequestReceivedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
			
			if(request == AccessRequestType.CONCURRENT_READ_BLOCKING) 
			{
				if(resourceNames.containsKey(resourceName)) 
				{
					Resource currResource = resourceNames.get(resourceName);
					
					//check if resource is pending disablement
					if(pendingDisable.get(currResource) == true)
					{
						requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
						log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
					}
					else if(resourceNames.get(resourceName).getStatus() == ResourceStatus.ENABLED)
					{
						if(writeAccess.get(currResource).isEmpty()) 
						{
							//Resource is not occupied by a writer
							readAccess.get(currResource).add(requestingUser);
							log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
							requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
						}
						else if(writeAccess.get(currResource).peek().equals(requestingUser) || 
								readAccess.get(currResource).contains(requestingUser))
						{
							//Re-entrant case!
							readAccess.get(currResource).add(requestingUser);
							log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
							requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
						}
						else
						{
							//resource is being written by someone else, Add to write Queue
							resourceQueues.get(currResource).add(requestingUser);
							queueMessages.get(currResource).add((AccessRequestMsg) msg);
						}
					}
					else
					{
						//resource is disabled, DENY
						requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
						log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
					}
				}
				else
				{
					if(resourceMap.containsKey(resourceName))
					{
						//resource manager is known
						ActorRef targetRM = resourceMap.get(resourceName);
						log(LogMsg.makeAccessRequestForwardedLogMsg(getSelf(), targetRM, ((AccessRequestMsg) msg).getAccessRequest()));
						targetRM.tell(msg, getSelf());
					}
					else
					{
						//resource manager is not known
						//Resource is not local or does not exist!
						ActorRef targetRM;
						
						if(!remoteMessages.containsKey(resourceName))
						{
							remoteMessages.put(resourceName, new HashMap<ActorRef, Object>());
						}
						
						remoteMessages.get(resourceName).put(requestingUser, msg);
						resourceCounts.put(resourceName, resourceManagers.size());
						
						for(int i = 0; i < resourceManagers.size(); i++)
						{
							targetRM = resourceManagers.get(i);
							targetRM.tell(new WhoHasResourceRequestMsg(resourceName, requestingUser), getSelf());
						}
						
					}
				}
				
			}
			else if(request == AccessRequestType.CONCURRENT_READ_NONBLOCKING)
			{
				if(resourceNames.containsKey(resourceName)) 
				{
					Resource currResource = resourceNames.get(resourceName);
					
					//check if resource is pending disablement
					if(pendingDisable.get(currResource) == true)
					{
						requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
						log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
					}
					else if(resourceNames.get(resourceName).getStatus() == ResourceStatus.ENABLED)
					{
						if(writeAccess.get(currResource).peek() == null) 
						{
							//Resource is not occupied by a writer
							readAccess.get(currResource).add(requestingUser);
							log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
							requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
						}
						else if(writeAccess.get(currResource).peek().equals(requestingUser) || 
								readAccess.get(currResource).contains(requestingUser))
						{
							//Re-entrant case!
							readAccess.get(currResource).add(requestingUser);
							log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
							requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
						}
						else
						{
							//resource is being written by someone else! Deny
							requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_BUSY), getSelf());
							log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_BUSY));
						}
					}
					else
					{
						//resource is disabled, to deny request
						requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
						log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
					}
				}
				else
				{
					//Resource is not local or does not exist!
					if(resourceMap.containsKey(resourceName))
					{
						//resource manager is known
						ActorRef targetRM = resourceMap.get(resourceName);
						log(LogMsg.makeAccessRequestForwardedLogMsg(getSelf(), targetRM, ((AccessRequestMsg) msg).getAccessRequest()));
						targetRM.tell(new AccessRequestMsg(((AccessRequestMsg) msg).getAccessRequest() , requestingUser), getSelf());
					}
					else
					{
						//resource manager is not known
						//Resource is not local or does not exist!
						ActorRef targetRM;
						
						if(!remoteMessages.containsKey(resourceName))
						{
							remoteMessages.put(resourceName, new HashMap<ActorRef, Object>());
						}
						
						remoteMessages.get(resourceName).put(requestingUser, msg);
						resourceCounts.put(resourceName, resourceManagers.size());
						
						for(int i = 0; i < resourceManagers.size(); i++)
						{
							targetRM = resourceManagers.get(i);
							targetRM.tell(new WhoHasResourceRequestMsg(resourceName, requestingUser), getSelf());
						}
					}
				}
			}
			else if(request == AccessRequestType.EXCLUSIVE_WRITE_BLOCKING) 
			{
				if(resourceNames.containsKey(resourceName)) 
				{
					Resource currResource = resourceNames.get(resourceName);
					
					//check if resource is pending disablement
					if(pendingDisable.get(currResource) == true)
					{
						requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
						log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
					}
					else if(resourceNames.get(resourceName).getStatus() == ResourceStatus.ENABLED)
					{
						if(writeAccess.get(currResource).isEmpty() && readAccess.get(currResource).isEmpty()) 
						{
							//Resource is not occupied by a writer
							writeAccess.get(currResource).add(requestingUser);
							log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
							requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
						}
						else if(!writeAccess.get(currResource).isEmpty() && writeAccess.get(currResource).peek().equals(requestingUser))
						{
							//Re-entrant case!
							writeAccess.get(currResource).add(requestingUser);
							log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
							requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
						}
						else if(readAccess.get(currResource).contains(requestingUser) &&
								(writeAccess.get(currResource).peek() == null || writeAccess.get(currResource).contains(requestingUser))) 
						{
							//this user is the only one with read access to this resource
							//check that this is only user in readAccess
							boolean readClear = true;
							for(int i = 0; i < readAccess.get(currResource).size(); i++) 
							{
								if(!readAccess.get(currResource).get(i).equals(requestingUser))
								{
									//there is another user currently holding reading access
									readClear = false;
								}
							}
							
							if(!readClear)
							{
								//another user is reading, add to resource queue
								resourceQueues.get(currResource).add(requestingUser);
								queueMessages.get(currResource).add((AccessRequestMsg) msg);
							}
							else
							{
								//user is the only one reading, no one else is writing
								writeAccess.get(currResource).add(requestingUser);
								log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
								requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
							}
						}
						else
						{
							//resource is being written by someone else! add to resource queue
							resourceQueues.get(currResource).add(requestingUser);
							queueMessages.get(currResource).add((AccessRequestMsg) msg);
						}
					}
					else
					{
						//resource is disabled, to deny request
						requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
						log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
					}
				}
				else
				{
					//Resource is not local or does not exist!
					if(resourceMap.containsKey(resourceName))
					{
						//resource manager is known
						ActorRef targetRM = resourceMap.get(resourceName);
						log(LogMsg.makeAccessRequestForwardedLogMsg(getSelf(), targetRM, ((AccessRequestMsg) msg).getAccessRequest()));
						targetRM.tell(new AccessRequestMsg(((AccessRequestMsg) msg).getAccessRequest() , requestingUser), getSelf());
					}
					else
					{
						//resource manager is not known
						//Resource is not local or does not exist!
						ActorRef targetRM;
						
						if(!remoteMessages.containsKey(resourceName))
						{
							remoteMessages.put(resourceName, new HashMap<ActorRef, Object>());
						}
						
						remoteMessages.get(resourceName).put(requestingUser, msg);
						resourceCounts.put(resourceName, resourceManagers.size()); 
						
						for(int i = 0; i < resourceManagers.size(); i++)
						{
							targetRM = resourceManagers.get(i);
							targetRM.tell(new WhoHasResourceRequestMsg(resourceName, requestingUser), getSelf());
						}
					}
				}
				
			}
			else if(request == AccessRequestType.EXCLUSIVE_WRITE_NONBLOCKING)
			{
				if(resourceNames.containsKey(resourceName)) 
				{
					Resource currResource = resourceNames.get(resourceName);
					
					//check if resource is pending disablement
					if(pendingDisable.get(currResource) == true)
					{
						requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
						log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
					}
					else if(resourceNames.get(resourceName).getStatus() == ResourceStatus.ENABLED)
					{
						if(writeAccess.get(currResource).peek() == null && readAccess.get(currResource).peek() == null) 
						{
							//Resource is not occupied by a writer
							writeAccess.get(currResource).add(requestingUser);
							log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
							requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
						}
						else if(writeAccess.get(currResource).peek().equals(requestingUser))
						{
							//Re-entrant case!
							writeAccess.get(currResource).add(requestingUser);
							log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
							requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
						}
						else if(readAccess.get(currResource).contains(requestingUser) &&
								(writeAccess.get(currResource).peek() == null || writeAccess.get(currResource).contains(requestingUser))) 
						{
							//this user is the only one with read access to this resource
							//check that this is only user in readAccess
							boolean readClear = true;
							for(int i = 0; i < readAccess.get(currResource).size(); i++) 
							{
								if(!readAccess.get(currResource).get(i).equals(requestingUser))
								{
									//there is another user currently holding reading access
									readClear = false;
								}
							}
							
							if(!readClear)
							{
								//another user is reading, deny access
								requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_BUSY), getSelf());
								log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_BUSY));
							}
							else
							{
								//user is the only one reading, no one else is writing
								writeAccess.get(currResource).add(requestingUser);
								log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest()));
								requestingUser.tell(new AccessRequestGrantedMsg((AccessRequestMsg) msg), getSelf());
							}
						}
						else
						{
							//resource is being written by someone else! Deny
							requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_BUSY), getSelf());
							log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_BUSY));
						}
					}
					else
					{
						//resource is disabled, to deny request
						requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) msg, AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
						log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) msg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
					}
				}
				else
				{
					//Resource is not local or does not exist!
					if(resourceMap.containsKey(resourceName))
					{
						//resource manager is known
						ActorRef targetRM = resourceMap.get(resourceName);
						log(LogMsg.makeAccessRequestForwardedLogMsg(getSelf(), targetRM, ((AccessRequestMsg) msg).getAccessRequest()));
						targetRM.tell(new AccessRequestMsg(((AccessRequestMsg) msg).getAccessRequest() , requestingUser), getSelf());
					}
					else
					{
						//resource manager is not known
						//Resource is not local or does not exist!
						ActorRef targetRM;
						
						if(!remoteMessages.containsKey(resourceName))
						{
							remoteMessages.put(resourceName, new HashMap<ActorRef, Object>());
						}
						
						remoteMessages.get(resourceName).put(requestingUser, msg);
						resourceCounts.put(resourceName, resourceManagers.size());
						
						for(int i = 0; i < resourceManagers.size(); i++)
						{
							targetRM = resourceManagers.get(i);
							targetRM.tell(new WhoHasResourceRequestMsg(resourceName, requestingUser), getSelf());
						}
						
						
					}
				}
			}
			else
			{
				System.out.println("Error, AccessRequestMsg is invalid type?");
			}
			
		}
		else if(msg instanceof ManagementRequestMsg)
		{
			ManagementRequestType request = ((ManagementRequestMsg) msg).getRequest().getType();
			String resourceName = ((ManagementRequestMsg) msg).getRequest().getResourceName();
			Resource currResource = resourceNames.get(resourceName);
			ActorRef requestingUser = ((ManagementRequestMsg) msg).getReplyTo();
			
			log(LogMsg.makeManagementRequestReceivedLogMsg(requestingUser, getSelf(), ((ManagementRequestMsg) msg).getRequest()));
			
			if(resourceNames.containsKey(resourceName))
			{
				//resource is local to this resourceManager
				if(request == ManagementRequestType.ENABLE)
				{
					currResource.enable();
					pendingDisable.put(currResource, false);
					log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), resourceName, ResourceStatus.ENABLED));
					requestingUser.tell(new ManagementRequestGrantedMsg((ManagementRequestMsg) msg), getSelf());
					log(LogMsg.makeManagementRequestGrantedLogMsg(requestingUser, getSelf(), ((ManagementRequestMsg) msg).getRequest()));
				}
				else if(request == ManagementRequestType.DISABLE)
				{
					if(readAccess.get(currResource).contains(requestingUser) || writeAccess.get(currResource).contains(requestingUser))
					{
						//requesting user currently holds access rights, Deny
						requestingUser.tell(new ManagementRequestDeniedMsg((ManagementRequestMsg) msg, ManagementRequestDenialReason.ACCESS_HELD_BY_USER), getSelf());
						log(LogMsg.makeManagementRequestDeniedLogMsg(requestingUser, getSelf(), ((ManagementRequestMsg) msg).getRequest(), ManagementRequestDenialReason.ACCESS_HELD_BY_USER));
					}
					else
					{
						//start pending disablement
						pendingDisable.put(currResource, true);
						ActorRef tempActor;
						AccessRequestMsg accessMsg;
						
						//clear the resource's queue
						while(!resourceQueues.get(currResource).isEmpty())
						{
							tempActor = resourceQueues.get(currResource).pop();
							accessMsg = queueMessages.get(currResource).pop();
							tempActor.tell(new AccessRequestDeniedMsg(accessMsg, AccessRequestDenialReason.RESOURCE_DISABLED), getSelf());
							log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), accessMsg.getAccessRequest(), AccessRequestDenialReason.RESOURCE_DISABLED));
						}
						
						if(writeAccess.get(currResource).isEmpty() && readAccess.get(currResource).isEmpty())
						{
							//resource can be disabled now
							currResource.disable();
							log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), resourceName, ResourceStatus.DISABLED));
							requestingUser.tell(new ManagementRequestGrantedMsg(((ManagementRequestMsg) msg).getRequest()), getSelf());
							log(LogMsg.makeManagementRequestGrantedLogMsg(requestingUser, getSelf(), ((ManagementRequestMsg) msg).getRequest()));
						}
						else
						{
							//wait for resource to stop being accessed and grant request
							awaitingDisable.get(currResource).add(requestingUser);
							disableMessages.get(currResource).add((ManagementRequestMsg) msg);
						}
					}
					
				}
				else
				{
					//error
					System.out.println("Error in management request type!");
				}
			}
			else
			{
				//Resource is not local to the resource manager
				if(resourceMap.containsKey(resourceName))
				{
					//resource manager is known
					ActorRef targetRM = resourceMap.get(resourceName);
					log(LogMsg.makeManagementRequestForwardedLogMsg(getSelf(), targetRM, ((ManagementRequestMsg) msg).getRequest()));
					targetRM.tell(msg, getSelf());
				}
				else
				{
					//resource manager is not known
					//Resource is not local or does not exist!
					ActorRef targetRM;
					
					if(!remoteMessages.containsKey(resourceName))
					{
						remoteMessages.put(resourceName, new HashMap<ActorRef, Object>());
					}
					
					remoteMessages.get(resourceName).put(requestingUser, msg);
					resourceCounts.put(resourceName, resourceManagers.size());
					
					for(int i = 0; i < resourceManagers.size(); i++)
					{
						targetRM = resourceManagers.get(i);
						targetRM.tell(new WhoHasResourceRequestMsg(resourceName, requestingUser), getSelf());
					}
					
				}
			}
			
		}
		else if(msg instanceof AccessReleaseMsg)
		{
			AccessType type = ((AccessReleaseMsg) msg).getAccessRelease().getType();
			String resourceName = ((AccessReleaseMsg) msg).getAccessRelease().getResourceName();
			Resource currResource = resourceNames.get(resourceName);
			ActorRef requestingUser = ((AccessReleaseMsg) msg).getSender();
			
			
			log(LogMsg.makeAccessReleaseReceivedLogMsg(requestingUser, getSelf(), ((AccessReleaseMsg) msg).getAccessRelease()));
			
			
			if(resourceNames.containsKey(resourceName))
			{
				if(type == AccessType.CONCURRENT_READ)
				{
					if(readAccess.get(currResource).contains(requestingUser))
					{
						readAccess.get(currResource).remove(requestingUser);
					}
					else
					{
						//user does not have write access, ignore!
						log(LogMsg.makeAccessReleaseIgnoredLogMsg(requestingUser, getSelf(), ((AccessReleaseMsg) msg).getAccessRelease()));
					}
				}
				else if(type == AccessType.EXCLUSIVE_WRITE)
				{
					if(writeAccess.get(currResource).contains(requestingUser))
					{
						writeAccess.get(currResource).remove(requestingUser);
					}
					else
					{
						//user does not have write access, ignore!
						System.out.println(writeAccess.get(currResource).toString());
						log(LogMsg.makeAccessReleaseIgnoredLogMsg(requestingUser, getSelf(), ((AccessReleaseMsg) msg).getAccessRelease()));
					}
				}
				else
				{
					System.out.println("Error in access release type!");
				}
			
				if(readAccess.get(currResource).isEmpty() && writeAccess.get(currResource).isEmpty())
				{
					//check if access has been released on resource
					log(LogMsg.makeAccessReleasedLogMsg(requestingUser, getSelf(), ((AccessReleaseMsg) msg).getAccessRelease()));
				}
				
				if(pendingDisable.get(currResource))
				{
					//check to see if access was released on resource since disablement is pending
					if(readAccess.get(currResource).isEmpty() && writeAccess.get(currResource).isEmpty())
					{
						//resource can now be disabled
						log(LogMsg.makeResourceStatusChangedLogMsg(getSelf(), resourceName, ResourceStatus.DISABLED));
						currResource.disable();
						
						//reply to users awaiting disablement
						ActorRef tempActor;
						ManagementRequestMsg requestMsg;
						
						while(!awaitingDisable.get(currResource).isEmpty())
						{
							tempActor = awaitingDisable.get(currResource).pop();
							requestMsg = disableMessages.get(currResource).pop();
							tempActor.tell(new ManagementRequestGrantedMsg(requestMsg), getSelf());
							log(LogMsg.makeManagementRequestGrantedLogMsg(tempActor, getSelf(), requestMsg.getRequest()));
						}
					}
				}else {
					//disablement is not pending so give access to next User in queue
					if(readAccess.get(currResource).isEmpty() && writeAccess.get(currResource).isEmpty())
					{
						//access is lifted so give access to next user in queue
						if(!resourceQueues.get(currResource).isEmpty())
						{
							ActorRef nextUser = resourceQueues.get(currResource).pop();
							AccessRequestMsg accessMsg = queueMessages.get(currResource).pop();
							
							if(accessMsg.getAccessRequest().getType() == AccessRequestType.CONCURRENT_READ_BLOCKING ||
							   accessMsg.getAccessRequest().getType() == AccessRequestType.CONCURRENT_READ_NONBLOCKING)
							{
								readAccess.get(currResource).add(nextUser);
								nextUser.tell(new AccessRequestGrantedMsg(accessMsg), getSelf());
								log(LogMsg.makeAccessRequestGrantedLogMsg(requestingUser, getSelf(), accessMsg.getAccessRequest()));
								
								accessMsg = queueMessages.get(currResource).peek();
								while(!resourceQueues.get(currResource).isEmpty() &&
								      (accessMsg.getAccessRequest().getType() == AccessRequestType.CONCURRENT_READ_BLOCKING ||
									  accessMsg.getAccessRequest().getType() == AccessRequestType.CONCURRENT_READ_NONBLOCKING))
								{
									nextUser = resourceQueues.get(currResource).pop();
									accessMsg = queueMessages.get(currResource).pop();
									readAccess.get(currResource).add(nextUser);
									log(LogMsg.makeAccessRequestGrantedLogMsg(nextUser, getSelf(), accessMsg.getAccessRequest()));
									nextUser.tell(new AccessRequestGrantedMsg(accessMsg), getSelf());
									
									if(!queueMessages.get(currResource).isEmpty())
									{
										accessMsg = queueMessages.get(currResource).peek();
									}
								}
							}
							else if(accessMsg.getAccessRequest().getType() == AccessRequestType.EXCLUSIVE_WRITE_BLOCKING ||
									accessMsg.getAccessRequest().getType() == AccessRequestType.EXCLUSIVE_WRITE_NONBLOCKING)
							{
								writeAccess.get(currResource).add(nextUser);
								log(LogMsg.makeAccessRequestGrantedLogMsg(nextUser, getSelf(), ((AccessRequestMsg) accessMsg).getAccessRequest()));
								nextUser.tell(new AccessRequestGrantedMsg(accessMsg), getSelf());
							}
							else
							{
								System.out.println("error getting queue request type (String)!");
							}
						}
					}
				}
			}
			else 
			{
				//resource is not local to this resource manager
				if(resourceMap.containsKey(resourceName))
				{
					//resource manager is known
					ActorRef targetRM = resourceMap.get(resourceName);
					log(LogMsg.makeAccessReleaseForwardedLogMsg(getSelf(), targetRM, ((AccessReleaseMsg) msg).getAccessRelease()));
					targetRM.tell(msg, getSelf());
				}
				else
				{
					//resource manager is not known
					//Resource is not local or does not exist!
					ActorRef targetRM;
					
					if(!remoteMessages.containsKey(resourceName))
					{
						remoteMessages.put(resourceName, new HashMap<ActorRef, Object>());
					}
					
					remoteMessages.get(resourceName).put(requestingUser, msg);
					resourceCounts.put(resourceName, resourceManagers.size());
					
					for(int i = 0; i < resourceManagers.size(); i++)
					{
						targetRM = resourceManagers.get(i);
						targetRM.tell(new WhoHasResourceRequestMsg(resourceName, requestingUser), getSelf());
					}
				}
			}
			
		}
		else if(msg instanceof WhoHasResourceRequestMsg)
		{
			String resourceName = ((WhoHasResourceRequestMsg) msg).getResourceName();
			
			if(resourceNames.containsKey(resourceName))
			{
				//this resource manager has local access to the resource
				sender.tell(new WhoHasResourceResponseMsg(resourceName, true, ((WhoHasResourceRequestMsg) msg).getRequestingUser()), getSelf());
			}
			else
			{
				//the resource is not local to this resource manager
				sender.tell(new WhoHasResourceResponseMsg(resourceName, false, ((WhoHasResourceRequestMsg) msg).getRequestingUser()), getSelf());
			}
		}
		else if(msg instanceof WhoHasResourceResponseMsg)
		{
			String resourceName = ((WhoHasResourceResponseMsg) msg).getResourceName();
			ActorRef requestingUser = ((WhoHasResourceResponseMsg) msg).getRequestingUser();
			Object requestMsg = remoteMessages.get(resourceName).get(requestingUser);
			
			if(((WhoHasResourceResponseMsg) msg).getResult())
			{
				//resource was found in this resource manager
				resourceMap.put(resourceName, sender);
				log(LogMsg.makeRemoteResourceDiscoveredLogMsg(getSelf(), sender, resourceName));
				sender.tell(requestMsg, getSelf());
			}
			else
			{
				//resource was not found in this resource manager
				resourceCounts.put(resourceName, resourceCounts.get(resourceName) - 1);
				if(resourceCounts.get(resourceName) < 1)
				{
					//no resourceManager has the resource, deny
					if(requestMsg instanceof AccessRequestMsg)
					{
						requestingUser.tell(new AccessRequestDeniedMsg((AccessRequestMsg) requestMsg, AccessRequestDenialReason.RESOURCE_NOT_FOUND), getSelf());
						log(LogMsg.makeAccessRequestDeniedLogMsg(requestingUser, getSelf(), ((AccessRequestMsg) requestMsg).getAccessRequest(), AccessRequestDenialReason.RESOURCE_NOT_FOUND));
					}
					else if(requestMsg instanceof ManagementRequestMsg)
					{
						requestingUser.tell(new ManagementRequestDeniedMsg((ManagementRequestMsg) requestMsg, ManagementRequestDenialReason.RESOURCE_NOT_FOUND), getSelf());
						log(LogMsg.makeManagementRequestDeniedLogMsg(requestingUser, getSelf(), ((ManagementRequestMsg) requestMsg).getRequest(), ManagementRequestDenialReason.RESOURCE_NOT_FOUND));
					}
					else
					{
						System.out.println("Error in remoteMessages msg type!");
					}
				}
			}
		}
		else
		{
			System.out.println("Error, invalid message?: " + msg.getClass());
		}
		
	}catch(Exception e) {
		System.out.println(e.toString());
	}
	
	
	}
}
