#include "ns3/core-module.h"
#include "ns3/log.h"
#include "ns3/simulator.h"
#include "ns3/config.h"
#include "ns3/object.h"
#include "ns3/timer.h"
#include "ns3/socket.h"
#include "ns3/socket-factory.h"
#include "ns3/tcp-socket-factory.h"

#include "flexflow-app.h"

namespace ns3 {

NS_LOG_COMPONENT_DEFINE ("FlexFlowApplication");
NS_OBJECT_ENSURE_REGISTERED (FlexFlowApplication);

FlexFlowApplication::FlexFlowApplication() {
  NS_LOG_FUNCTION (this);
}

FlexFlowApplication::~FlexFlowApplication() {

}

TypeId FlexFlowApplication::GetTypeId() {
  static TypeId tid = TypeId ("ns3::FlexFlowApplication")
    .SetParent<Application> ()
    .AddConstructor<FlexFlowApplication> ();

  return tid;
}

// Ptr<Socket> FlexFlowApplication::GetSocket() {
//   return m_socket;
// }

void FlexFlowApplication::AddTask(Ptr<FlexFlowTask> task) {
  NS_LOG_FUNCTION (this << task);
  tasks[task->guid] = task;
}

void FlexFlowApplication::StartInitTasks() {
  NS_LOG_FUNCTION (this);
  // uint64_t delta = 0;
  int count = 0;
  NS_LOG_INFO(this << " has " << tasks.size() << " pending tasks");
  for (auto task: tasks) {
    //std::cerr << "guid:" << task.second->guid << "size: " << task.second->preTasks.size() << std::endl;
    if (task.second->preTasks.size() == 0) {
      Simulator::ScheduleNow(&FlexFlowTask::TaskStart, &(*task.second));
      count++;
    }
  }
  NS_LOG_INFO(this << " added " << count << " init tasks.");
}

void FlexFlowApplication::AddPeer(int hostId, Address rAddr, Address lAddr, Ptr<FlexFlowApplication> hostApp) {
  NS_LOG_FUNCTION(this << hostId << rAddr << lAddr << hostApp);

  auto remoteAddrIter = peers.find(hostId);
  
  if (remoteAddrIter == peers.end()) {
    PeerFFInfo peer;
    peer.id = hostId;
    peer.app = hostApp;
    peer.peerAddr = rAddr;
    peer.localAddr = lAddr;
    
    peers[hostId] = peer;
  } else {
    NS_FATAL_ERROR("peer already added!");
  }
  
}

void FlexFlowApplication::NotifyTaskComplete(int taskId) {
  NS_LOG_FUNCTION(this << taskId);

  for (auto & task: tasks[taskId]->nextTasks) {
    task->preTasks.erase(taskId);
    Simulator::ScheduleNow(&FlexFlowTask::TaskStart, task);
  }

}

void FlexFlowApplication::DoDispose () {
  NS_LOG_FUNCTION (this);

  peers.clear();
  m_socketList.clear ();

  // chain up
  Application::DoDispose ();
}

void FlexFlowApplication::StartApplication () {
  NS_LOG_FUNCTION (this);
  // Create the socket if not already

  // for (auto & peer: peers) {
  // auto & m_socket = peer.second.socket;
  Address m_local = InetSocketAddress (Ipv4Address::GetAny (), 50000);
    
  m_socket = Socket::CreateSocket (GetNode (), TcpSocketFactory::GetTypeId());
  if (m_socket->Bind (m_local) == -1) {
    NS_FATAL_ERROR ("Failed to bind socket, errno " << m_socket->GetErrno());
  }
  m_socket->Listen ();
  m_socket->ShutdownSend ();
    
  m_socket->SetRecvCallback (MakeCallback (&FlexFlowApplication::HandleRead, this));
  m_socket->SetAcceptCallback (
    MakeNullCallback<bool, Ptr<Socket>, const Address &> (),
    MakeCallback (&FlexFlowApplication::HandleAccept, this));
  m_socket->SetCloseCallbacks (
    MakeCallback (&FlexFlowApplication::HandlePeerClose, this),
    MakeCallback (&FlexFlowApplication::HandlePeerError, this));
  // }

  StartInitTasks();
}

void FlexFlowApplication::StopApplication () {
   NS_LOG_FUNCTION (this);
  while(!m_socketList.empty ()) {
    Ptr<Socket> acceptedSocket = m_socketList.front ();
    m_socketList.pop_front ();
    acceptedSocket->Close ();
  }
  for (auto & peer: peers) {
    auto & m_socket = peer.second.socket;
    if (m_socket) {
      m_socket->Close ();
      m_socket->SetRecvCallback (MakeNullCallback<void, Ptr<Socket> > ());
    }
  }
}

void FlexFlowApplication::HandleRead (Ptr<Socket> socket)
{
  NS_LOG_FUNCTION (this << socket);
  Ptr<Packet> packet;
  Address from;
  Address localAddress;
  while ((packet = socket->RecvFrom (from))) {
    if (packet->GetSize () == 0) { //EOF
      break;
    }
    m_totalRx += packet->GetSize ();
    if (InetSocketAddress::IsMatchingType (from)) {
      NS_LOG_INFO ("At time " << Simulator::Now ().GetSeconds ()
                    << "s packet sink received "
                    <<  packet->GetSize () << " bytes from "
                    << InetSocketAddress::ConvertFrom(from).GetIpv4 ()
                    << " port " << InetSocketAddress::ConvertFrom (from).GetPort ()
                    << " total Rx " << m_totalRx << " bytes");
    } else if (Inet6SocketAddress::IsMatchingType (from)) {
      NS_LOG_INFO ("At time " << Simulator::Now ().GetSeconds ()
                    << "s packet sink received "
                    <<  packet->GetSize () << " bytes from "
                    << Inet6SocketAddress::ConvertFrom(from).GetIpv6 ()
                    << " port " << Inet6SocketAddress::ConvertFrom (from).GetPort ()
                    << " total Rx " << m_totalRx << " bytes");
    }
    socket->GetSockName (localAddress);
    // m_rxTrace (packet, from);
    // m_rxTraceWithAddresses (packet, from, localAddress);

  }
}

void FlexFlowApplication::HandlePeerClose (Ptr<Socket> socket)
{
  NS_LOG_FUNCTION (this << socket);
}
 
void FlexFlowApplication::HandlePeerError (Ptr<Socket> socket)
{
  NS_LOG_FUNCTION (this << socket);
}

void FlexFlowApplication::HandleAccept (Ptr<Socket> s, const Address& from)
{
  NS_LOG_FUNCTION (this << s << from);
  s->SetRecvCallback (MakeCallback (&FlexFlowApplication::HandleRead, this));
  m_socketList.push_back (s);
}


void FlexFlowTask::Connect(PeerFFInfo & peer) {
  NS_LOG_FUNCTION (this);

  m_socket = Socket::CreateSocket(ffapp->GetNode(), TcpSocketFactory::GetTypeId());
  int ret = -1;

  InetSocketAddress localInetSocket = InetSocketAddress(Ipv4Address::ConvertFrom(peer.localAddr));
  if (!peer.localAddr.IsInvalid()) {
    NS_ABORT_MSG_IF ((Inet6SocketAddress::IsMatchingType (peer.peerAddr) && InetSocketAddress::IsMatchingType (peer.localAddr)) ||
                      (InetSocketAddress::IsMatchingType (peer.peerAddr) && Inet6SocketAddress::IsMatchingType (peer.localAddr)),
                      "Incompatible peer and local address IP version");
    ret = m_socket->Bind (localInetSocket);
  }
  else {
    if (Inet6SocketAddress::IsMatchingType (peer.peerAddr)) {
      ret = m_socket->Bind6 ();
    }
    else if (InetSocketAddress::IsMatchingType (peer.peerAddr)) {
      ret = m_socket->Bind ();
    }
  }

  if (ret == -1) {
    NS_FATAL_ERROR ("Failed to bind socket");
  }

  m_socket->Connect (InetSocketAddress(Ipv4Address::ConvertFrom(peer.peerAddr), 50000));
  m_socket->ShutdownRecv ();
  m_socket->SetConnectCallback (
    MakeCallback (&FlexFlowTask::ConnectionSucceeded, this),
    MakeCallback (&FlexFlowTask::ConnectionFailed, this));
  m_socket->SetSendCallback (
    MakeCallback (&FlexFlowTask::DataSend, this));
  m_socket->SetCloseCallbacks (
    MakeCallback(&FlexFlowTask::ConnectionClosed, this),
    MakeCallback(&FlexFlowTask::ConnectionClosedError, this));

  // NS_LOG_INFO("Connection succeeded");
}

void FlexFlowTask::ConnectionClosed (Ptr<Socket> socket) {
  NS_LOG_FUNCTION (this << socket);
  // NS_LOG_FUNCTION (this << socket
  Simulator::ScheduleNow(&FlexFlowTask::CleanUp, this);
}

void FlexFlowTask::ConnectionClosedError (Ptr<Socket> socket) {
  NS_LOG_FUNCTION (this << socket);
  NS_FATAL_ERROR("ERROR socket close!");
}

void FlexFlowTask::ConnectionSucceeded (Ptr<Socket> socket) {
  NS_LOG_FUNCTION (this << socket);
  NS_LOG_INFO ("FlexFlowApplication Connection succeeded");
  std::cerr << "At time " << Simulator::Now().GetSeconds() << " task " << guid << " connected,"
            << " sending " << m_maxBytes << " bytes of data" << std::endl;
  m_connected = true;
  Address from, to;
  socket->GetSockName (from);
  socket->GetPeerName (to);
  SendData (from, to);
}

void FlexFlowTask::ConnectionFailed (Ptr<Socket> socket) {
  NS_LOG_FUNCTION (this << socket);
  NS_FATAL_ERROR ("FlexFlowTask, Connection Failed");
}

void FlexFlowTask::DataSend (Ptr<Socket> socket, uint32_t)
{
  NS_LOG_FUNCTION (this);

  if (m_connected) { // Only send new data if the connection has completed
    Address from, to;
    socket->GetSockName (from);
    socket->GetPeerName (to);
    SendData (from, to);
  }
}

void FlexFlowTask::SendData (const Address &from, const Address &to)
{
  NS_LOG_FUNCTION (this);

  while (m_totBytes < m_maxBytes)
  { // Time to send more

    // uint64_t to allow the comparison later.
    // the result is in a uint32_t range anyway, because
    // m_sendSize is uint32_t.
    uint64_t toSend = m_sendSize;
    // Make sure we don't send too many
    if (m_maxBytes > 0)
    {
      toSend = std::min (toSend, m_maxBytes - m_totBytes);
    }

    NS_LOG_LOGIC ("sending packet at " << Simulator::Now ());

    Ptr<Packet> packet;
    if (m_unsentPacket) {
      packet = m_unsentPacket;
    } else {
      packet = Create<Packet> (toSend);
    }

    int actual = m_socket->Send (packet);
    if ((unsigned) actual == toSend) {
      m_totBytes += actual;
      // m_txTrace (packet);
      m_unsentPacket = 0;
    } else if (actual == -1) {
      // We exit this loop when actual < toSend as the send side
      // buffer is full. The "DataSent" callback will pop when
      // some buffer space has freed up.
      NS_LOG_DEBUG ("Unable to send packet; caching for later attempt");
      m_unsentPacket = packet;
      break;
    } else {
      NS_FATAL_ERROR ("Unexpected return value from m_socket->Send ()");
    }
  }
  // Check if time to close (all sent)
  if (m_totBytes == m_maxBytes && m_connected) {
    m_socket->Close ();
    m_connected = false;
    // Simulator::ScheduleNow(&FlexFlowTask::CleanUp, this);
  }
}

FlexFlowTask::FlexFlowTask() {

}

FlexFlowTask::FlexFlowTask(Ptr<FlexFlowApplication> ffapp, FlexFlowTask::FlexFlowTaskType type) 
 : ffapp(ffapp), type(type) {
  m_sendSize = 1424;
  started = m_connected = false;
  readyTime = startTime = computeTime = 0;
  workerId = guid = fromWorker = toWorker = fromGuid = toGuid = fromNode = toNode = 0;
  m_totBytes = m_maxBytes = 0;
  m_unsentPacket = 0;
}

TypeId FlexFlowTask::GetTypeId() {
  static TypeId tid = TypeId ("ns3::FlexFlowTask")
    .SetParent<Object> ()
    .AddConstructor<FlexFlowTask> ();
  return tid;
}

void FlexFlowTask::AddPrevTask(Ptr<FlexFlowTask> task) {
  preTasks[task->guid] = task;
}

void FlexFlowTask::AddNextTask(Ptr<FlexFlowTask> task) {
  nextTasks.push_back(task);
}

void FlexFlowTask::TaskStart() {
  NS_LOG_INFO("Guid: " << guid << " try to start at " << Simulator::Now().GetSeconds());
  if (preTasks.size() != 0 || started) {
    NS_LOG_INFO(guid << "can't start, pre.size = " << preTasks.size());
    return;
  }
  std::cerr << "At time " << Simulator::Now().GetSeconds() << " task " << guid << " started" << std::endl;
  // sim_start = Simulator::Now().GetPicoSeconds();
  started = true;
  // std::cerr << "started" << std::endl;

  if (type == FlexFlowTask::FF_COMM) {
    Connect(ffapp->peers[toWorker]);
  } 
  else {
    // sim_duration = (simtime_picosec)((double)computeTime * 1000000000ULL); 
    Simulator::Schedule(Time(NanoSeconds(computeTime * 1000000)), &FlexFlowTask::CleanUp, this);
  }
}

void FlexFlowTask::CleanUp() {
  NS_LOG_INFO ("At time " << Simulator::Now ().GetSeconds () << " Task " << guid << " finished");
  std::cerr << "At time " << Simulator::Now ().GetSeconds () << " Task " << guid << " finished" << std::endl;
  ffapp->NotifyTaskComplete(this->guid);
  m_socket = 0;
}

}