/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.interpreter.remote;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.thrift.TException;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.helium.ApplicationEventListener;
import org.apache.zeppelin.interpreter.InterpreterContextRunner;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.RemoteZeppelinServerResource;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterEvent;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterEventType;
import org.apache.zeppelin.interpreter.thrift.RemoteInterpreterService.Client;
import org.apache.zeppelin.interpreter.thrift.ZeppelinServerResourceParagraphRunner;
import org.apache.zeppelin.resource.Resource;
import org.apache.zeppelin.resource.ResourceId;
import org.apache.zeppelin.resource.ResourcePool;
import org.apache.zeppelin.resource.ResourceSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Processes message from RemoteInterpreter process
 */
public class RemoteInterpreterEventPoller extends Thread {
  private static final Logger logger = LoggerFactory.getLogger(RemoteInterpreterEventPoller.class);
  private final ScheduledExecutorService appendService =
      Executors.newSingleThreadScheduledExecutor();
  private final RemoteInterpreterProcessListener listener;
  private final ApplicationEventListener appListener;

  private volatile boolean shutdown;

  private RemoteInterpreterProcess interpreterProcess;
  private InterpreterGroup interpreterGroup;

  public RemoteInterpreterEventPoller(
      RemoteInterpreterProcessListener listener,
      ApplicationEventListener appListener) {
    this.listener = listener;
    this.appListener = appListener;
    shutdown = false;
  }

  public void setInterpreterProcess(RemoteInterpreterProcess interpreterProcess) {
    this.interpreterProcess = interpreterProcess;
  }

  public void setInterpreterGroup(InterpreterGroup interpreterGroup) {
    this.interpreterGroup = interpreterGroup;
  }

  @Override
  public void run() {
    Client client = null;
    AppendOutputRunner runner = new AppendOutputRunner(listener);
    ScheduledFuture<?> appendFuture = appendService.scheduleWithFixedDelay(
        runner, 0, AppendOutputRunner.BUFFER_TIME_MS, TimeUnit.MILLISECONDS);

    while (!shutdown) {
      // wait and retry
      if (!interpreterProcess.isRunning()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // nothing to do
        }
        continue;
      }

      try {
        client = interpreterProcess.getClient();
      } catch (Exception e1) {
        logger.error("Can't get RemoteInterpreterEvent", e1);
        waitQuietly();
        continue;
      }

      RemoteInterpreterEvent event = null;
      boolean broken = false;
      try {
        event = client.getEvent();
      } catch (TException e) {
        broken = true;
        logger.error("Can't get RemoteInterpreterEvent", e);
        waitQuietly();
        continue;
      } finally {
        interpreterProcess.releaseClient(client, broken);
      }

      Gson gson = new Gson();

      AngularObjectRegistry angularObjectRegistry = interpreterGroup.getAngularObjectRegistry();

      try {
        if (event.getType() == RemoteInterpreterEventType.NO_OP) {
          continue;
        } else if (event.getType() == RemoteInterpreterEventType.ANGULAR_OBJECT_ADD) {
          AngularObject angularObject = gson.fromJson(event.getData(), AngularObject.class);
          angularObjectRegistry.add(angularObject.getName(),
              angularObject.get(), angularObject.getNoteId(), angularObject.getParagraphId());
        } else if (event.getType() == RemoteInterpreterEventType.ANGULAR_OBJECT_UPDATE) {
          AngularObject angularObject = gson.fromJson(event.getData(),
              AngularObject.class);
          AngularObject localAngularObject = angularObjectRegistry.get(
              angularObject.getName(), angularObject.getNoteId(), angularObject.getParagraphId());
          if (localAngularObject instanceof RemoteAngularObject) {
            // to avoid ping-pong loop
            ((RemoteAngularObject) localAngularObject).set(
                angularObject.get(), true, false);
          } else {
            localAngularObject.set(angularObject.get());
          }
        } else if (event.getType() == RemoteInterpreterEventType.ANGULAR_OBJECT_REMOVE) {
          AngularObject angularObject = gson.fromJson(event.getData(), AngularObject.class);
          angularObjectRegistry.remove(angularObject.getName(), angularObject.getNoteId(),
                  angularObject.getParagraphId());
        } else if (event.getType() == RemoteInterpreterEventType.RUN_INTERPRETER_CONTEXT_RUNNER) {
          InterpreterContextRunner runnerFromRemote = gson.fromJson(
              event.getData(), RemoteInterpreterContextRunner.class);

          listener.onRemoteRunParagraph(
              runnerFromRemote.getNoteId(), runnerFromRemote.getParagraphId());

        } else if (event.getType() == RemoteInterpreterEventType.RESOURCE_POOL_GET_ALL) {
          ResourceSet resourceSet = getAllResourcePoolExcept();
          sendResourcePoolResponseGetAll(resourceSet);
        } else if (event.getType() == RemoteInterpreterEventType.RESOURCE_GET) {
          String resourceIdString = event.getData();
          ResourceId resourceId = gson.fromJson(resourceIdString, ResourceId.class);
          logger.debug("RESOURCE_GET {} {}", resourceId.getResourcePoolId(), resourceId.getName());
          Object o = getResource(resourceId);
          sendResourceResponseGet(resourceId, o);
        } else if (event.getType() == RemoteInterpreterEventType.OUTPUT_APPEND) {
          // on output append
          Map<String, String> outputAppend = gson.fromJson(
                  event.getData(), new TypeToken<Map<String, Object>>() {}.getType());
          String noteId = (String) outputAppend.get("noteId");
          String paragraphId = (String) outputAppend.get("paragraphId");
          int index = Integer.parseInt(outputAppend.get("index"));
          String outputToAppend = (String) outputAppend.get("data");

          String appId = (String) outputAppend.get("appId");

          if (appId == null) {
            runner.appendBuffer(noteId, paragraphId, index, outputToAppend);
          } else {
            appListener.onOutputAppend(noteId, paragraphId, index, appId, outputToAppend);
          }
        } else if (event.getType() == RemoteInterpreterEventType.OUTPUT_UPDATE_ALL) {
          Map<String, Object> outputUpdate = gson.fromJson(
              event.getData(), new TypeToken<Map<String, Object>>() {}.getType());
          String noteId = (String) outputUpdate.get("noteId");
          String paragraphId = (String) outputUpdate.get("paragraphId");

          // clear the output
          List<Map<String, String>> messages =
              (List<Map<String, String>>) outputUpdate.get("messages");

          if (messages != null) {
            listener.onOutputClear(noteId, paragraphId);
            for (int i = 0; i < messages.size(); i++) {
              Map<String, String> m = messages.get(i);
              InterpreterResult.Type type =
                  InterpreterResult.Type.valueOf((String) m.get("type"));
              String outputToUpdate = (String) m.get("data");

              listener.onOutputUpdated(noteId, paragraphId, i, type, outputToUpdate);
            }
          }
        } else if (event.getType() == RemoteInterpreterEventType.OUTPUT_UPDATE) {
          // on output update
          Map<String, String> outputAppend = gson.fromJson(
              event.getData(), new TypeToken<Map<String, Object>>() {}.getType());
          String noteId = (String) outputAppend.get("noteId");
          String paragraphId = (String) outputAppend.get("paragraphId");
          int index = Integer.parseInt(outputAppend.get("index"));
          InterpreterResult.Type type =
              InterpreterResult.Type.valueOf((String) outputAppend.get("type"));
          String outputToUpdate = (String) outputAppend.get("data");
          String appId = (String) outputAppend.get("appId");

          if (appId == null) {
            listener.onOutputUpdated(noteId, paragraphId, index, type, outputToUpdate);
          } else {
            appListener.onOutputUpdated(noteId, paragraphId, index, appId, type, outputToUpdate);
          }
        } else if (event.getType() == RemoteInterpreterEventType.APP_STATUS_UPDATE) {
          // on output update
          Map<String, String> appStatusUpdate = gson.fromJson(
              event.getData(), new TypeToken<Map<String, String>>() {}.getType());

          String noteId = appStatusUpdate.get("noteId");
          String paragraphId = appStatusUpdate.get("paragraphId");
          String appId = appStatusUpdate.get("appId");
          String status = appStatusUpdate.get("status");

          appListener.onStatusChange(noteId, paragraphId, appId, status);
        } else if (event.getType() == RemoteInterpreterEventType.REMOTE_ZEPPELIN_SERVER_RESOURCE) {
          RemoteZeppelinServerResource reqResourceBody = gson.fromJson(
              event.getData(), RemoteZeppelinServerResource.class);
          progressRemoteZeppelinControlEvent(
              reqResourceBody.getResourceType(), listener, reqResourceBody);

        } else if (event.getType() == RemoteInterpreterEventType.META_INFOS) {
          Map<String, String> metaInfos = gson.fromJson(event.getData(),
              new TypeToken<Map<String, String>>() {
              }.getType());
          String id = interpreterGroup.getId();
          int indexOfColon = id.indexOf(":");
          String settingId = id.substring(0, indexOfColon);
          listener.onMetaInfosReceived(settingId, metaInfos);
        }
        logger.debug("Event from remote process {}", event.getType());
      } catch (Exception e) {
        logger.error("Can't handle event " + event, e);
      }
    }
    if (appendFuture != null) {
      appendFuture.cancel(true);
    }
  }

  private void progressRemoteZeppelinControlEvent(
      RemoteZeppelinServerResource.Type resourceType,
      RemoteInterpreterProcessListener remoteWorksEventListener,
      RemoteZeppelinServerResource reqResourceBody) throws Exception {
    boolean broken = false;
    final Gson gson = new Gson();
    final String eventOwnerKey = reqResourceBody.getOwnerKey();
    Client interpreterServerMain = null;
    try {
      interpreterServerMain = interpreterProcess.getClient();
      final Client eventClient = interpreterServerMain;
      if (resourceType == RemoteZeppelinServerResource.Type.PARAGRAPH_RUNNERS) {
        final List<ZeppelinServerResourceParagraphRunner> remoteRunners = new LinkedList<>();

        ZeppelinServerResourceParagraphRunner reqRunnerContext =
            new ZeppelinServerResourceParagraphRunner();

        Map<String, Object> reqResourceMap = (Map<String, Object>) reqResourceBody.getData();
        String noteId = (String) reqResourceMap.get("noteId");
        String paragraphId = (String) reqResourceMap.get("paragraphId");

        reqRunnerContext.setNoteId(noteId);
        reqRunnerContext.setParagraphId(paragraphId);

        RemoteInterpreterProcessListener.RemoteWorksEventListener callBackEvent =
            new RemoteInterpreterProcessListener.RemoteWorksEventListener() {

              @Override
              public void onFinished(Object resultObject) {
                boolean clientBroken = false;
                if (resultObject != null && resultObject instanceof List) {
                  List<InterpreterContextRunner> runnerList =
                      (List<InterpreterContextRunner>) resultObject;
                  for (InterpreterContextRunner r : runnerList) {
                    remoteRunners.add(
                        new ZeppelinServerResourceParagraphRunner(r.getNoteId(), r.getParagraphId())
                    );
                  }

                  final RemoteZeppelinServerResource resResource =
                      new RemoteZeppelinServerResource();
                  resResource.setOwnerKey(eventOwnerKey);
                  resResource.setResourceType(RemoteZeppelinServerResource.Type.PARAGRAPH_RUNNERS);
                  resResource.setData(remoteRunners);

                  try {
                    eventClient.onReceivedZeppelinResource(gson.toJson(resResource));
                  } catch (Exception e) {
                    clientBroken = true;
                    logger.error("Can't get RemoteInterpreterEvent", e);
                    waitQuietly();
                  } finally {
                    interpreterProcess.releaseClient(eventClient, clientBroken);
                  }
                }
              }

              @Override
              public void onError() {
                logger.info("onGetParagraphRunners onError");
              }
            };

        remoteWorksEventListener.onGetParagraphRunners(
            reqRunnerContext.getNoteId(), reqRunnerContext.getParagraphId(), callBackEvent);
      }
    } catch (Exception e) {
      broken = true;
      logger.error("Can't get RemoteInterpreterEvent", e);
      waitQuietly();

    } finally {
      interpreterProcess.releaseClient(interpreterServerMain, broken);
    }
  }

  private void sendResourcePoolResponseGetAll(ResourceSet resourceSet) {
    Client client = null;
    boolean broken = false;
    try {
      client = interpreterProcess.getClient();
      List<String> resourceList = new LinkedList<>();
      Gson gson = new Gson();
      for (Resource r : resourceSet) {
        resourceList.add(gson.toJson(r));
      }
      client.resourcePoolResponseGetAll(resourceList);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      broken = true;
    } finally {
      if (client != null) {
        interpreterProcess.releaseClient(client, broken);
      }
    }
  }

  private ResourceSet getAllResourcePoolExcept() {
    ResourceSet resourceSet = new ResourceSet();
    for (InterpreterGroup intpGroup : InterpreterGroup.getAll()) {
      if (intpGroup.getId().equals(interpreterGroup.getId())) {
        continue;
      }

      RemoteInterpreterProcess remoteInterpreterProcess = intpGroup.getRemoteInterpreterProcess();
      if (remoteInterpreterProcess == null) {
        ResourcePool localPool = intpGroup.getResourcePool();
        if (localPool != null) {
          resourceSet.addAll(localPool.getAll());
        }
      } else if (interpreterProcess.isRunning()) {
        Client client = null;
        boolean broken = false;
        try {
          client = remoteInterpreterProcess.getClient();
          List<String> resourceList = client.resourcePoolGetAll();
          Gson gson = new Gson();
          for (String res : resourceList) {
            resourceSet.add(gson.fromJson(res, Resource.class));
          }
        } catch (Exception e) {
          logger.error(e.getMessage(), e);
          broken = true;
        } finally {
          if (client != null) {
            intpGroup.getRemoteInterpreterProcess().releaseClient(client, broken);
          }
        }
      }
    }
    return resourceSet;
  }



  private void sendResourceResponseGet(ResourceId resourceId, Object o) {
    Client client = null;
    boolean broken = false;
    try {
      client = interpreterProcess.getClient();
      Gson gson = new Gson();
      String rid = gson.toJson(resourceId);
      ByteBuffer obj;
      if (o == null) {
        obj = ByteBuffer.allocate(0);
      } else {
        obj = Resource.serializeObject(o);
      }
      client.resourceResponseGet(rid, obj);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      broken = true;
    } finally {
      if (client != null) {
        interpreterProcess.releaseClient(client, broken);
      }
    }
  }

  private Object getResource(ResourceId resourceId) {
    InterpreterGroup intpGroup = InterpreterGroup.getByInterpreterGroupId(
        resourceId.getResourcePoolId());
    if (intpGroup == null) {
      return null;
    }
    RemoteInterpreterProcess remoteInterpreterProcess = intpGroup.getRemoteInterpreterProcess();
    if (remoteInterpreterProcess == null) {
      ResourcePool localPool = intpGroup.getResourcePool();
      if (localPool != null) {
        return localPool.get(resourceId.getName());
      }
    } else if (interpreterProcess.isRunning()) {
      Client client = null;
      boolean broken = false;
      try {
        client = remoteInterpreterProcess.getClient();
        ByteBuffer res = client.resourceGet(
            resourceId.getNoteId(),
            resourceId.getParagraphId(),
            resourceId.getName());
        Object o = Resource.deserializeObject(res);
        return o;
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
        broken = true;
      } finally {
        if (client != null) {
          intpGroup.getRemoteInterpreterProcess().releaseClient(client, broken);
        }
      }
    }
    return null;
  }

  private void waitQuietly() {
    try {
      synchronized (this) {
        wait(1000);
      }
    } catch (InterruptedException ignored) {
      logger.info("Error in RemoteInterpreterEventPoller while waitQuietly : ", ignored);
    }
  }

  public void shutdown() {
    shutdown = true;
    synchronized (this) {
      notify();
    }
  }
}
