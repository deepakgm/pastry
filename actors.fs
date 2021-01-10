namespace pastry

open System
open Akka.FSharp
open System.Collections.Generic


module Pastry  =
    let b = 2
    let arrLength = 8  // 'l' value in paper

    let range = 1 <<< b
    let rangeL = (uint64 range)
    
    [<AllowNullLiteral>]
    type NodeId (random:Random,idStr:string) =
        let s1 = idStr.Split(".")
        let genId (i:int) = 
            if (isNull random) then
                (int s1.[i])
                // (int idStr.[i]- int '0')
            else  random.Next(0,range)

        let id1:list<int> = List.init arrLength (fun i -> (genId(i)))

        let idStr = if idStr = "" then String.Join( '.', id1) else idStr

        let add (list1:list<int>,num:int)=
            let newList:List<int> = new List<int>(arrLength);
            for num in list1 do newList.Add(num)
            let mutable index = arrLength - 1
            let mutable carry:int = num
            while carry > 0 && index > 0 do 
                if list1.[index] + carry < range then 
                    newList.[index] <- list1.[index] + carry
                    carry <- 0
                else
                    newList.[index] <- (list1.[index] + carry) % range
                    carry <- 1
                index <- index - 1
            let newList2:list<int> = List.init arrLength (fun i -> (newList.[i]))
            newList2
        
        let remove (list1:list<int>,num:int)=
            let newList:List<int> = new List<int>(arrLength);
            for num in list1 do newList.Add(num)        
            let mutable index = arrLength - 1
            let mutable carry = num
            while carry > 0 && index > 0 do 
                if list1.[index] - carry >= 0 then 
                    newList.[index] <- list1.[index] - carry
                    carry <- 0
                else
                    newList.[index] <- (list1.[index] - carry) + range
                    carry <- 1
                index <- index - 1
            let newList2:list<int> = List.init arrLength (fun i -> (newList.[i]))
            newList2

        let difference (l1:list<int>,l2:list<int>) = 
            let rec loop (sum:int, mult:int, carry:bool, i:int) =
                // printfn "val loop %i sum %i mult %i carry %b" i sum mult carry
                if i = -1 then sum
                else
                    let n1 = if carry then l1.[i]-1 else l1.[i]
                    let n2 = l2.[i]
                    // printfn "n1:%i n2:%i" n1 n2
                    if n1 < n2 then
                        loop ((sum + mult*(n1+range-n2)),mult*range,true,i-1)
                    else 
                        loop ((sum + mult*(n1-n2)),mult*range,false,i-1)
            loop (0,1,false,arrLength-1)

        let lastNodeToFirstNodeDifference (l1:list<int>) = (range-l1.[arrLength-1])
       
        let absLowL = Array.init arrLength (fun _ -> 0UL)
        let absHighL = Array.init arrLength (fun _ -> (uint64 (range-1)))

        let circularDistance (list1:list<int>,list2:list<int>) = 
            let mutable isList1Greater = true
            let rec loop(i:int) = 
                    if i =arrLength then false
                    else if list2.[i] = list1.[i] then loop (i+1)
                    else if list2.[i] < list1.[i] then false
                    else true
            let mutable arr1:uint64[] = Array.zeroCreate arrLength
            let mutable arr2:uint64[] = Array.zeroCreate arrLength
            if loop 0 then 
                arr2 <- (Array.init arrLength (fun i -> (uint64 list1.[i]))) 
                arr1 <- (Array.init arrLength (fun i -> (uint64 list2.[i]))) 
                isList1Greater <- false
            else 
                arr1 <- (Array.init arrLength (fun i -> (uint64 list1.[i]))) 
                arr2 <- (Array.init arrLength (fun i -> (uint64 list2.[i]))) 
           
            let rec loop1 (newArr1:uint64[],newArr2:uint64[],sum:uint64, mult:uint64, carry:bool, i:int) =
                if i = -1 then sum
                else
                    let n1 = if carry then newArr1.[i]-1UL else newArr1.[i]
                    let n2 = newArr2.[i]
                    if n1 < n2 then
                        loop1 (newArr1,newArr2,(sum + mult*(n1+(uint64 range)-n2)),mult*(uint64 range),true,i-1)
                    else 
                        loop1 (newArr1,newArr2,(sum + mult*(n1-n2)),mult*(uint64 range),false,i-1)
            let normalDistance = loop1 (arr1,arr2,0UL,1UL,false,arrLength-1)
            let backwardDistance = loop1 (absHighL,arr1,0UL,1UL,false,arrLength-1) + loop1 (arr2,absLowL,0UL,1UL,false,arrLength-1) + 1UL
            if normalDistance > backwardDistance then (backwardDistance, not isList1Greater )else (normalDistance, isList1Greater)            
        override this.GetHashCode() =
            hash (id1)
        override this.Equals(nodeB) =
            match nodeB with
            | :? NodeId as nodeB -> id1 = nodeB.Id
            | _ -> false

        member this.Id = id1
        member this.String = idStr
        member this.CommonPrefix (b:NodeId)= 
            let mutable flag = true
            let mutable index = 0
            while flag do
                if index < arrLength && this.Id.[index] = b.Id.[index] then index <- index+1
                else flag <- false
            index    
        member this.CircularDistance (nodeB:NodeId) = 
            circularDistance(id1,nodeB.Id)   
        member this.CircularDistance2 (nodeB:NodeId) = 
            let dist,_ = circularDistance(id1,nodeB.Id)
            dist  
    type OptionalNodeId = NodeId option

    type RandomNodeIdGenerator () = 
        let random = Random ()
        let list = new List<String>()
        member this.Gen = 
            let mutable nodeId = NodeId(random,"")
            while list.Exists(fun elem -> elem = nodeId.String) do
                nodeId <- NodeId(random,"")
            list.Add(nodeId.String)
            nodeId
        member this.GetRandom(limit:int) = 
            NodeId(null,list.[random.Next(limit)])

    type CustomNodeIdGenerator () =
        let random = Random()
        let list = new List<String>()
        let mutable index = 1UL
        let digitToString (num:uint64) =
            let mutable digit:uint64 = num
            let mutable strId = ""
            for i in 1..arrLength do
                if i <> 1 then strId <- "." + strId
                strId <- (string (digit % rangeL))+strId
                digit <- (digit/rangeL)
            strId
        member this.Reset = index <- 0UL;
        member this.Gen =
            let strId = digitToString index
            index <- index+1UL
            NodeId(null,strId)
        member this.GetPrev =
            let strId = digitToString (index-2UL)
            NodeId(null,strId)
        member this.GetRandom(limit:int) = 
            let strId = digitToString (uint64 (random.Next limit))
            NodeId(null,strId)
        member this.GetRandom(node:NodeId) =
            let rec getVal (i:int)(sum:int) = if i=arrLength then sum else getVal (i+1) (sum*range+node.Id.[i]) 
            let strId = digitToString (uint64 (random.Next (getVal 0 0)))
            NodeId(null,strId)
        member this.GetRandom () =
            let strId = digitToString (uint64 0)
            NodeId(null,strId)

    let idGenerator = CustomNodeIdGenerator()

    type RoutingTable (myId:NodeId) =
        let mutable data:NodeId[][] = Array.init arrLength (fun i -> Array.zeroCreate range)
        // do for i in 1..arrLength do data.Add(new List<NodeId>()) 
        member this.GetRow (row:int) = data.[row]
        member this.GetAll = data
        member this.Add(joingNodeId:NodeId) = 
            let commonPrefix = myId.CommonPrefix joingNodeId
            if isNull data.[commonPrefix].[joingNodeId.Id.[commonPrefix]] then
                Array.set data.[commonPrefix] (joingNodeId.Id.[commonPrefix]) joingNodeId
        member this.Print = 
            printfn "RoutingTable of node:%s" myId.String
            let mutable rowIndex = 0
            for row in data do
                printf "ROW:%i" rowIndex
                for nodeId in row do
                    if not (isNull nodeId) then 
                        printf " %s " nodeId.String
                printfn ""
                rowIndex <- rowIndex + 1
        member this.GetRoutableNode (targetNodeId:NodeId) =
            let commonPrefix = myId.CommonPrefix(targetNodeId)
            let refVal = targetNodeId.Id.[commonPrefix] 
            let mutable resultNode:OptionalNodeId = None
            if not (isNull data.[commonPrefix].[refVal]) then 
                resultNode <- (Some data.[commonPrefix].[refVal] )       
            resultNode
        member this.CopyRow (refList:NodeId[]) = 
            for refNode in refList do
                if not(isNull refNode) && refNode <> myId then
                    this.Add(refNode)
        member this.CopyData (refData:NodeId[][]) = 
            for row in refData do
                this.CopyRow(row)

    type LeafSet (myId:NodeId) =
        let length = range/2
        let mutable data:List<NodeId> = new List<NodeId>()
        let mutable size = 0
        let mutable isChanged = false
        let mutable maxDist = UInt64.MaxValue
        let mutable maxDistNode:NodeId = null    
        let updateMaxDist () = 
            if size < (2*length) then maxDist <- UInt64.MaxValue
            else 
                maxDist <- 0UL
                for elem in data do
                    let dist,_ = myId.CircularDistance(elem)
                    if dist > maxDist then maxDist <- dist; maxDistNode <- elem
        let exists (node:NodeId) = data.Exists(fun elem -> elem=node)
        member this.GetAll = data
        member this.Add(joingNodeId:NodeId) =
            if not (isNull joingNodeId) && not (exists joingNodeId)then
                let dist, _ = myId.CircularDistance(joingNodeId)
                if dist < maxDist then
                    data.Add(joingNodeId)
                    if size = (2*length) then
                        data.Remove(maxDistNode) |> ignore 
                    else size <- size+1 
                    updateMaxDist()
                    isChanged <- true    
        member this.Print = 
            printfn "LeafSet of node:%s" myId.String
            for nodeId in data do if not (isNull nodeId) then printf " %s " nodeId.String
            printfn ""
        member this.Size = size
        member this.IsWithinRange (nodeA:NodeId,nodeB:NodeId)=
            let dist,_ = nodeA.CircularDistance(nodeB)
            dist <= maxDist
            // dist <= lengthL    
        member this.GetClosest(targetNode:NodeId) = 
            let mutable result:OptionalNodeId = None
            let mutable minDist= UInt64.MaxValue
            let mutable dummy = false
            // for i in 0..(data.Length-1) do
            for i in 0..(size-1) do
                if not (isNull data.[i]) then
                    let dist,_ = targetNode.CircularDistance(data.[i])
                    if dist < minDist then 
                        minDist <- dist
                        result <- (Some data.[i])
            result
        member this.CopyData (refSet:LeafSet) = 
            for i in 0..(refSet.Size-1) do
                if not (isNull refSet.GetAll.[i]) && refSet.GetAll.[i] <> myId then 
                    this.Add refSet.GetAll.[i]   
        member this.IsChanged = isChanged 
        member this.SetIsChanged = isChanged <- false

    type NeighbourSet (myId:NodeId) =
        let data:NodeId[] = Array.zeroCreate arrLength
        let mutable index = 0
        let random = Random()
        member this.HasCapacity = index <= arrLength-1
        member this.Exists (nodeId:NodeId) = 
            let mutable result = false
            for elem in data do
                if not(isNull elem) && elem = nodeId then 
                    result <- true
            result
        member this.AddRandomly(joingNodeId:NodeId) = 
            if this.HasCapacity && random.Next(arrLength) <= 1 then
                if not (this.Exists joingNodeId) then 
                    Array.set data index joingNodeId
                    index <- index+1
        member this.Print = 
            printfn "NeighbourSet of node:%s" myId.String
            for nodeId in data do if not(isNull nodeId) then printf " %s " nodeId.String
            printfn ""
        member this.GetAll = data

    type Command =
    | Join of joiningNode:NodeId
    | Leave of from:NodeId
    | NewLeaf of routingTable:RoutingTable * refLeafSet:LeafSet
    | JoinAcknowledge of from:NodeId * routingTable:RoutingTable * refLeafSet:LeafSet
    | Message of fromNodeId:NodeId * targetNodeId:NodeId * numOfHops:int * value:string
    | MessageDelivered of numOfHops:int
    | Print
    | Dummy
    // Messages realted to tracker
    | Joined of from:NodeId
    | StartSendingMessages
    | Finished of from:NodeId * numOfHops:double


    let peer (myId:NodeId) (numNodes:int) (numRequests:int) (bootStrapPeerId:NodeId) (mailbox:Actor<_>) =  
        let routingTable = RoutingTable(myId)
        let leafSet = LeafSet(myId) 
        let neighbourSet = NeighbourSet(myId) 
        let random = Random()
        let peerTracker = (select "/user/peerTracker" mailbox.Context.System)
        let sendMsgToPeer (nodeId:NodeId, msg:Command) = 
            (select (sprintf "/user/peer-%s" nodeId.String) mailbox.Context.System) <! msg
        
        let pastryInit () =
            if not (isNull bootStrapPeerId) then  
                let bootStrapPeer = (select (sprintf "/user/peer-%s" bootStrapPeerId.String) mailbox.Context.System)
                bootStrapPeer <! (Join myId)

        let getClosestAmongAll (refNode:NodeId) = 
            let mutable resultNode:OptionalNodeId = None
            let curCommonPrefix = myId.CommonPrefix(refNode)
            let mutable minDist = UInt64.MaxValue
            // go through all known node find one with minimum dist
            for row in routingTable.GetAll do
                for elem in row do
                    if not(isNull elem) && elem.CommonPrefix(refNode) >= curCommonPrefix then
                        let dist,_ = elem.CircularDistance(refNode) 
                        if dist < minDist then 
                            resultNode <- (Some elem)
                            minDist <- dist
            for elem in leafSet.GetAll do
                    if not(isNull elem) && elem.CommonPrefix(refNode) >= curCommonPrefix then
                        let dist,_ = elem.CircularDistance(refNode) 
                        if dist < minDist then 
                            resultNode <- (Some elem)
                            minDist <- dist
            for elem in neighbourSet.GetAll do
                    if not(isNull elem) && elem.CommonPrefix(refNode) >= curCommonPrefix then
                        let dist,_ = elem.CircularDistance(refNode) 
                        if dist < minDist then 
                            resultNode <- (Some elem)
                            minDist <- dist  
            resultNode

        let routeMessage (message:Command,targetNodeId:NodeId) =
            if leafSet.IsWithinRange(myId,targetNodeId) then 
                match leafSet.GetClosest(targetNodeId) with
                | Some nextNodeId ->
                    sendMsgToPeer (nextNodeId,message)
                    true
                | None -> 
                        false
            else
                match routingTable.GetRoutableNode(targetNodeId) with
                | Some nextNodeId -> 
                    sendMsgToPeer (nextNodeId,message)
                    true
                    // printfn "sent to a routable node %s" nextNodeId.String
                | None -> 
                    match getClosestAmongAll targetNodeId with 
                    | Some nextNodeId -> 
                        sendMsgToPeer (nextNodeId,message)
                        true
                    | None ->
                        false
                  
        let addNodeInfo (refNode:NodeId) = 
            neighbourSet.AddRandomly(refNode)
            if refNode = myId then
                printfn "error: same node!"
            else if leafSet.IsWithinRange (myId,refNode) then
                    leafSet.Add(refNode)
            else
                routingTable.Add(refNode)
           
        let insertion (joiningNode:NodeId) = 
            if not (routeMessage (Join (joiningNode), joiningNode)) then peerTracker <! (Joined myId)
            sendMsgToPeer (joiningNode,(JoinAcknowledge(myId,routingTable,leafSet)))
            addNodeInfo joiningNode

        let leafGossipLoop () =
            if leafSet.IsChanged then 
                let gossipMsg = NewLeaf (routingTable,leafSet)
                for nodeId in leafSet.GetAll do
                    if not (isNull nodeId) then
                        sendMsgToPeer (nodeId, gossipMsg)
                // let prev = leafSet.GetPrev
                // match prev with 
                // | Some nodeId ->  
                //     // printfn "%s sending gossip to %s" myId.String nodeId.String
                //     sendMsgToPeer (nodeId, gossipMsg)
                // | None -> ()
                // let next = leafSet.GetNext  
                // match next with 
                // | Some nodeId ->  
                //     // printfn "%s sending gossip to %s" myId.String nodeId.String
                //     sendMsgToPeer (nodeId, gossipMsg)
                // | None -> ()
                leafSet.SetIsChanged
                sendMsgToPeer (idGenerator.GetRandom(myId), (NewLeaf (routingTable,leafSet)))


        let pipeToWithSender recipient sender asyncComp = pipeTo asyncComp recipient sender
       
        let rec sendMessageToRandomPeer (count:int) = 
            let randomNodeId = idGenerator.GetRandom(numNodes)
            let mesaage = Message (myId, randomNodeId, 0 , "some random message" )
            routeMessage (mesaage, randomNodeId) |> ignore
            // Async.Sleep(1000) |> Async.RunSynchronously
            if  count < numRequests then sendMessageToRandomPeer (count+1)

        let rec loop ((msgDeliveredCount:double)) (prevAvgHops:double) = actor { 
            let! msg = mailbox.Receive()
            match msg with
            | Message (fromNodeId, targetNodeId, numOfHops, value) -> 
                if myId <> targetNodeId then 
                    if not (routeMessage ((Message (fromNodeId, targetNodeId, numOfHops+1, value) ), targetNodeId)) then
                         printfn "cannot deliver message from %s to %s. #hops:%i" fromNodeId.String targetNodeId.String numOfHops; routingTable.Print;leafSet.Print;neighbourSet.Print;
                else
                    sendMsgToPeer (fromNodeId, (MessageDelivered numOfHops))
            | MessageDelivered numOfHops ->
                    let avgHops = (prevAvgHops*msgDeliveredCount + (double numOfHops))/(msgDeliveredCount+1.0)
                    if (msgDeliveredCount+1.0) = (double numRequests) then
                        peerTracker <! Finished (myId,avgHops)
                    return! loop (msgDeliveredCount+1.0) avgHops
            | StartSendingMessages -> 
                // sendMessageToRandomPeer 0
                async { sendMessageToRandomPeer 0; return Dummy} |> pipeToWithSender mailbox.Self mailbox.Context.Sender
            | Leave from -> 
                () // implemented leave in failure model code
                // printfn "leave called"
            | Join joiningNode ->
                if joiningNode <> myId then
                    insertion joiningNode
                else 
                    peerTracker <! (Joined myId)
                leafGossipLoop ()
            | NewLeaf (refRoutingTable, refLeafSet) -> 
                leafSet.CopyData(refLeafSet)
                routingTable.CopyData(refRoutingTable.GetAll)
                leafGossipLoop ()
            | JoinAcknowledge (fromNode,refRoutingTable,refLeafSet) ->
                addNodeInfo fromNode
                if leafSet.IsWithinRange (myId,fromNode) then
                    leafSet.CopyData (refLeafSet)
                else
                    let commonPrefix = myId.CommonPrefix(fromNode)
                    routingTable.CopyRow (refRoutingTable.GetRow(commonPrefix))
            | Print -> routingTable.Print;leafSet.Print;neighbourSet.Print
            | _ -> ()

            return! loop msgDeliveredCount prevAvgHops
        }
        pastryInit()
        loop 0.0 0.0

    let peerTracker (numNodes:int)(numRequests:int) (mailbox:Actor<_>) =
        
        let rec trackMessageDelivery (prevAvgHops:double)(count:double) = actor {
            let! msg = mailbox.Receive()
            match msg with
            | Finished (fromNodeId, numOfHops) -> 
                 let avgHops = (prevAvgHops*count + (double numOfHops))/(count+1.0)
                 printfn "finish count:%f, avgHops: %f" count avgHops
                 if count+1.0 < (double numNodes) then 
                    return! trackMessageDelivery avgHops (count+1.0)
                 else 
                    printfn "Finished!\naverage number of hops %f" avgHops
                    Async.AwaitTask(mailbox.Context.System.Terminate()) |> ignore;
            | _ -> return! trackMessageDelivery prevAvgHops count
        }
        
        let triggerPeer () = actor {
            printfn "starting send random prints"
            Async.Sleep(5000) |> Async.RunSynchronously

            //let randomId1 = (idGenerator.GetRandom numNodes)
            //(select (sprintf "/user/peer-%s" (randomId1.String)) mailbox.Context.System) <! Print
            //Async.Sleep(1000) |> Async.RunSynchronously

            idGenerator.Reset
            for i in 1..numNodes do 
                let nodeId = idGenerator.Gen
                (select (sprintf "/user/peer-%s" (nodeId.String)) mailbox.Context.System) <! StartSendingMessages
            return! trackMessageDelivery 0.0 0.0
        }

        let rec trackJoin (count:int) = actor {
            if count < numNodes then
                let! msg = mailbox.Receive()
                match msg with
                | Joined _->
                    //  printfn "%i joined %s" count nodeId.String
                     return! trackJoin (count+1)
                | _ -> return! trackJoin (count)
            else
                printfn "all peers joined the network!" 
                return! triggerPeer ()
        }

        printfn "starting to track peers.."
        trackJoin 0    
    
    let Start (numNodes:int,numRequests:int) = 
        let random = Random ()
        printfn "hello there!"

        let myActorSystem = System.create "myActorSystem" (Configuration.load ())
        
        let peerTracker = spawn myActorSystem "peerTracker" (peerTracker numNodes numRequests)
        // create initial node
        printfn "starting peers.."
        let bootStrapPeerId = idGenerator.GetRandom()
        spawn myActorSystem (sprintf "peer-%s" bootStrapPeerId.String) (peer bootStrapPeerId numNodes numRequests null) |> ignore
        for i in 1..numNodes do
            let nodeId = idGenerator.Gen
            spawn myActorSystem (sprintf "peer-%s" nodeId.String) (peer nodeId numNodes numRequests bootStrapPeerId) |> ignore
            
        printfn "all peeers started!"

        
        myActorSystem.WhenTerminated.Wait ()
    
