import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class ServerNFS {
    private static String rootPath = "";
    private static int session = 1;
    private static int sessionFileId = 0;

    private static final int HEADER_MAX_SIZE = HeaderSize();
    private static final int PAYLOAD_MAX_SIZE = 5000;
    private static final int PACKET_SIZE = HEADER_MAX_SIZE+PAYLOAD_MAX_SIZE;
    private static final int BUFFER_SIZE = 5;
    private static final int TIMEOUT = 1000;

    private static boolean serverRun = true;
    private static DatagramSocket serverSocket;

    private static List<Long> openedFileIds;
    private static List<String> openedFileNames;
    private static List<FileChannel> openedFileChannels;

    public static void main(String[] args) {
        rootPath = args[0];
        File rootFolder = new File(rootPath);
        if(!rootFolder.exists()){
            System.out.println("Path doesn't exists");
            return;
        }
        else if(!rootFolder.isDirectory()){
            System.out.println("Path isn't directory");
            return;
        }

        File memoryFile = new File("ServerMemory.txt");
        try {
            if(memoryFile.createNewFile()){
                FileWriter fileWriter = new FileWriter(memoryFile);
                fileWriter.write(String.valueOf(session));
                fileWriter.close();
            }
            else {
                Scanner scanner = new Scanner(memoryFile);
                session = scanner.nextInt();
                if(session == Integer.MAX_VALUE){
                    session=0;
                }
                session++;
                FileWriter fileWriter = new FileWriter(memoryFile);
                fileWriter.write(String.valueOf(session));
                fileWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        openedFileIds = new ArrayList<>();
        openedFileNames = new ArrayList<>();
        openedFileChannels = new ArrayList<>();

        try {
            String hostAddress = InetAddress.getLocalHost().getHostAddress();
            InetSocketAddress myAddress = new InetSocketAddress(hostAddress,8000);
            serverSocket = new DatagramSocket(myAddress);
            //serverSocket.setSoTimeout(TIMEOUT);
            String localIP = InetAddress.getLocalHost().getHostAddress();
            System.out.println(localIP);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            serverRun = false;
            serverSocket.close();
            System.out.println("The server is shut down!");
        }));

        while (serverRun){
            ReceivePacket();
        }

        serverSocket.close();
    }

    private static int HeaderSize(){
        int longLength = String.valueOf(Long.MAX_VALUE).length();
        int intLength = String.valueOf(Integer.MAX_VALUE).length();
        int commas = 5;
        int operations = 5;

        int size = operations + 2*intLength + 2*longLength + commas;
//        System.out.println(size);
        // 2* because UTF-16
        return 2*size;
    }

    //all functionality in receive because we want the sender address
    public static void ReceivePacket(){
        byte[] packet_bytes = new byte[PACKET_SIZE];
        DatagramPacket rcv_packet = new DatagramPacket(packet_bytes, packet_bytes.length);

        try {
            serverSocket.receive(rcv_packet);
        } catch (IOException e) {
            return;
        }

        SocketAddress clientAddress = rcv_packet.getSocketAddress();

        int nonNullBytes = rcv_packet.getData().length;
        while (true) {
            if (nonNullBytes-- <= 0 || rcv_packet.getData()[nonNullBytes] != 0) break;
        }

        byte[] payloadBytes = new byte[nonNullBytes+1];
        System.arraycopy(rcv_packet.getData(), 0, payloadBytes, 0, nonNullBytes+1);

        String header = new String(rcv_packet.getData(), 0, nonNullBytes+1);
        //System.out.println("header" + header);
        String[] partedPayload = header.split(",");

        String operation = partedPayload[0];
        String requestNum = partedPayload[1];
        int frag;
        long fileId;
        long fdPos;
        int fdMoveLength;
        String replyPayload;

        switch (operation){
            case "open":
                String fileName = partedPayload[2];
                Integer[] flagsArray = new Integer[partedPayload.length-3];
                for (int i=0; i<flagsArray.length;i++){
                    flagsArray[i] = Integer.valueOf(partedPayload[i+3]);
                }
                List<Integer> flags = Arrays.asList(flagsArray);
                String openPayload = OpenFile(fileName, flags);

                replyPayload = operation+','+requestNum+','+openPayload;
                //SEND PACKET
                System.out.println(replyPayload);
                System.out.println("-----------------------");
                SendPacket(replyPayload.getBytes(), clientAddress);
                break;
            case "read":
                frag = Integer.parseInt(partedPayload[2]);
                fileId = Long.parseLong(partedPayload[3]);
                fdPos = Long.parseLong(partedPayload[4]);
                fdMoveLength = Integer.parseInt(partedPayload[5]);

                replyPayload = operation+','+requestNum+','+frag+',';
                System.out.println(replyPayload);
                byte[] replyHeader = replyPayload.getBytes();

                byte[] readPayload = ReadFile(fileId, fdPos, fdMoveLength);

                byte[] packet = new byte[replyHeader.length + readPayload.length];
                System.arraycopy(replyHeader, 0, packet, 0, replyHeader.length);
                System.arraycopy(readPayload, 0, packet, replyHeader.length, readPayload.length);
                //SEND PACKET
                System.out.println("-----------------------");
                SendPacket(packet, clientAddress);
                break;
            case "write":
                frag = Integer.parseInt(partedPayload[2]);
                fileId = Long.parseLong(partedPayload[3]);
                fdPos = Long.parseLong(partedPayload[4]);

                int offset = 0;
                for (int i = 0; i < 5; i++) {
                    offset += partedPayload[i].length() + 1;
                }

                byte[] toBeWritten = new byte[nonNullBytes+1 - offset];
                System.arraycopy(payloadBytes, offset, toBeWritten, 0,toBeWritten.length);

                String writePayload = WriteFile(fileId, fdPos, toBeWritten);

                replyPayload = operation+','+requestNum+','+frag+','+writePayload;
                //SEND PACKET
                System.out.println(replyPayload);
                System.out.println("-----------------------");
                SendPacket(replyPayload.getBytes(), clientAddress);
                break;
            default:
        }
    }

    public static void SendPacket(byte[] packetBytes, SocketAddress clientAddress){
        //byte[] packetBytes = payload.getBytes();
        DatagramPacket dp = new DatagramPacket(packetBytes, packetBytes.length, clientAddress);
        try {
            serverSocket.send(dp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String OpenFile(String fileName, List<Integer> flags){
        String filePathStr = rootPath+"/"+fileName;
        File file = new File(filePathStr);
        Path filePath = Paths.get(filePathStr);
        FileChannel fileChannel;
        Set<StandardOpenOption> openOptionSet = new HashSet<>();

        if(flags.contains(1)){
            openOptionSet.add(StandardOpenOption.CREATE);
            if(file.exists()){
                if(file.canRead()){
                    openOptionSet.add(StandardOpenOption.READ);
                }
                if(file.canWrite())
                    openOptionSet.add(StandardOpenOption.WRITE);
                else {
                    if (flags.contains(3)) {
                        openOptionSet.add(StandardOpenOption.TRUNCATE_EXISTING);
                        System.out.println(openOptionSet);
                        return "failed";
                    }
                }
            }
            else{
                openOptionSet.add(StandardOpenOption.READ);
                openOptionSet.add(StandardOpenOption.WRITE);
            }
        }
        if(flags.contains(2)){
            openOptionSet.add(StandardOpenOption.CREATE_NEW);
        }
        if(flags.contains(3)){
            openOptionSet.add(StandardOpenOption.TRUNCATE_EXISTING);
        }
        if(flags.contains(4)){
            openOptionSet.add(StandardOpenOption.READ);
            openOptionSet.add(StandardOpenOption.WRITE);
        }
        if(flags.contains(5)){
            openOptionSet.add(StandardOpenOption.READ);
        }
        if(flags.contains(6)){
            openOptionSet.add(StandardOpenOption.WRITE);
        }

        System.out.println(openOptionSet);

        if(flags.contains(5) && flags.contains(6))
            return "failed";
        if(flags.contains(4) && (flags.contains(5) || flags.contains(6)))
            return "failed";

        long fileSize;
        try {
            fileChannel = FileChannel.open(filePath, openOptionSet);
            fileSize = fileChannel.size();
        } catch (IOException e) {
            e.printStackTrace();
            //send packet failed
//            return "open,failed";
            return "failed";
        }

        Long fileId;

        if(openedFileNames.contains(fileName)){
            //MEANS THAT OPEN SUCCEEDED
            fileId = openedFileIds.get(openedFileNames.indexOf(fileName));
        }else{
            sessionFileId++;
            fileId = (((long)session) << 32) | (sessionFileId & 0xffffffffL);
            System.out.println("new " + fileId);
            /*int session = (int)(fileId >> 32);
            int fileId = (int)fileId;*/

            //memory config
            if(openedFileNames.size()>=BUFFER_SIZE){
                openedFileNames.remove(0);
                openedFileIds.remove(0);
                try {
                    openedFileChannels.get(0).close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                openedFileChannels.remove(0);
            }

            //add inode, filename, fileChannel
            openedFileNames.add(fileName);
            openedFileIds.add(fileId);
            openedFileChannels.add(fileChannel);
            System.out.println("opened " + openedFileNames);
        }

        char canRead = (file.canRead()) ? '1' : '0';
        char canWrite = (file.canWrite()) ? '1' : '0';

        return fileId.toString()+','+ fileSize +','+canRead+','+canWrite;

        /*if(fileIdsNames.containsValue(fileName)){
            //find id by udp
            int knownId = fileIdsNames.entrySet().stream()
                    .filter(entry -> fileName.equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst().get();
        }else{
            if(fileIdsNames.size()==BUFFER_SIZE){

            }
            fileIdsNames.put(0, fileName);
            openedIdsChannels.put(0, fileChannel);
        }*/
    }

    public static byte[] ReadFile(Long fileId, Long offset, int length){
        if(!openedFileIds.contains(fileId)){
            return "closed".getBytes();
        }

        FileChannel fileChannel = openedFileChannels.get(openedFileIds.indexOf(fileId));

        ByteBuffer readBuffer = ByteBuffer.allocate(length);

        int numOfBytesRead;
        long fileSize;

        try {
            numOfBytesRead = fileChannel.read(readBuffer, offset);
            fileSize = fileChannel.size();
        } catch (IOException e) {
            e.printStackTrace();
            return "failed".getBytes();
        }

        System.out.println("Read bytes " + numOfBytesRead);
        if(numOfBytesRead <0){
            return "error".getBytes();
        }/*else if(numOfBytesRead ==0){
            return "EOF";
        }*/

        byte[] header = (fileSize+",").getBytes();
        byte[] readBytes = new byte[numOfBytesRead];
        readBuffer.flip();
        readBuffer.get(readBytes);

        //String payload = new String(readBytes);
        //System.out.println("Server read: " + payload);


        //MAYBE DOESN'T NEED (PERMISSIONS)
        /*String fileName = openedFileNames.get(openedFileIds.indexOf(fileId));
        String filePathStr = rootPath+"/"+fileName;
        File file = new File(filePathStr);
        char canRead = (file.canRead()) ? '1' : '0';
        char canWrite = (file.canWrite()) ? '1' : '0';*/

        byte[] returnBytes = new byte[header.length + numOfBytesRead];
        System.arraycopy(header, 0, returnBytes, 0, header.length);
        System.arraycopy(readBytes, 0, returnBytes, header.length, numOfBytesRead);

        return returnBytes;
    }
    public static String WriteFile(Long fileId, Long offset, byte[] bytes){
        if(!openedFileIds.contains(fileId)){
            return "closed";
        }

        FileChannel fileChannel = openedFileChannels.get(openedFileIds.indexOf(fileId));

        ByteBuffer writeBuffer = ByteBuffer.allocate(bytes.length);
        writeBuffer.put(bytes);
        writeBuffer.flip();

        int numOfBytesWritten = 0;
        long fileSize = 0L;
        try {
            numOfBytesWritten = fileChannel.write(writeBuffer, offset);
            fileSize = fileChannel.size();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Written bytes " + numOfBytesWritten);
        if(numOfBytesWritten <0){
            return "error";
        }/*else if(numOfBytesWritten ==0){
            return "EOF";
        }*/

        //MAYBE DOESN'T NEED (PERMISSIONS)
        /*String fileName = openedFileNames.get(openedFileIds.indexOf(fileId));
        String filePathStr = rootPath+"/"+fileName;
        File file = new File(filePathStr);
        char canRead = (file.canRead()) ? '1' : '0';
        char canWrite = (file.canWrite()) ? '1' : '0';*/

        return fileSize+","+numOfBytesWritten;
    }
}
