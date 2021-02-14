import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class ClientNFS {
    private static MiddlewareNFS myMiddleware;
    private static boolean appRun = true;
    private static ArrayList<Integer> fileDescriptors;
    
    public static void main(String[] args) {
        String ipAddress = "127.0.1.1";
        int port = 8000;
        /*try {
            ipAddress = InetAddress.getLocalHost().getHostAddress();
            ipAddress = "127.0.1.1";
            NFS_init(ipAddress, port,10, 3000, 120L);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }*/
        
        fileDescriptors = new ArrayList<>();
        Scanner scanner = new Scanner(System.in);
        String userInput;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            appRun = false;
            if(myMiddleware!=null) {
                myMiddleware.Terminate();
                try {
                    myMiddleware.join(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("The client is shut down!");
        }));

        boolean endUI = false;

        while (appRun){
            System.out.println("--------------------------------------------");
            System.out.println("(i) Initialize\n" +
                    "(o) Open\n" +
                    "(r) Read\n" +
                    "(w) Write\n" +
                    "(s) Seek\n" +
                    "(c) Close\n" +
                    "(d) Copy Demo\n" +
                    "(p) Print FDs\n" +
                    "(pc) Print Cache\n" +
                    "(m) Metrics Demo\n" +
                    "(e) Exit");
            System.out.println("Your choice: ");
            userInput = scanner.nextLine();

            switch (userInput){
                case "i":
                    try {
                        System.out.println("How many cache blocks: ");
                        int cacheBlocks = scanner.nextInt();
                        scanner.nextLine(); //flush enter
                        System.out.println("Block size (bytes): ");
                        int blockSize = scanner.nextInt();
                        scanner.nextLine(); //flush enter
                        System.out.println("Fresh time (sec): ");
                        long freshT = scanner.nextLong();
                        scanner.nextLine(); //flush enter

                        NFS_init(ipAddress, port, cacheBlocks, blockSize, freshT);
                    }catch (InputMismatchException e){
                        System.out.println("Invalid input");
                    }catch (IOException e) {
                        e.printStackTrace();
                        return;
                    }
                    break;
                case "o":
                    if(myMiddleware == null){
                        System.out.println("You must initialize your NFS");
                        break;
                    }
                    System.out.println("File: ");
                    String filename = scanner.nextLine();
                    while(filename.isBlank()){
                        System.out.println("Please give file name: ");
                        filename = scanner.nextLine();
                    }

                    System.out.println("(1) O_CREAT\n" +
                            "(2) O_EXCL\n" +
                            "(3) O_TRUNC\n" +
                            "(4) O_RDWR\n" +
                            "(5) O_RDONLY\n" +
                            "(6) O_WRONLY");
                    System.out.println("Your flags (separator ','): ");
                    String flagsInput = scanner.nextLine();

                    while(flagsInput.isBlank()){
                        System.out.println("Please give flags: ");
                        flagsInput = scanner.nextLine();
                    }

                    NFS_open(filename, flagsInput);
                    break;
                case "r":
                    if(myMiddleware == null){
                        System.out.println("You must initialize your NFS");
                        break;
                    }
                    try {
                        System.out.println("File Descriptor: ");
                        int readFD = scanner.nextInt();
                        scanner.nextLine(); //flush enter
                        System.out.println("Bytes count for read: ");
                        int readLength = scanner.nextInt();
                        scanner.nextLine(); //flush enter

                        NFS_read(readFD, readLength);
                    }catch (InputMismatchException e){
                        System.out.println("Invalid input");
                    }
                    break;
                case "w":
                    if(myMiddleware == null){
                        System.out.println("You must initialize your NFS");
                        break;
                    }
                    try {
                        System.out.println("File Descriptor: ");
                        int writeFD = scanner.nextInt();
                        scanner.nextLine(); //flush enters
                        System.out.println("Write data: ");
                        String writeData = scanner.nextLine();

                        while(writeData.isEmpty()){
                            System.out.println("Please give writeData: ");
                            writeData = scanner.nextLine();
                        }

                        NFS_write(writeFD, writeData);
                    }catch (InputMismatchException e){
                        System.out.println("Invalid input");
                    }
                    break;
                case "s":
                    if(myMiddleware == null){
                        System.out.println("You must initialize your NFS");
                        break;
                    }
                    try {
                        System.out.println("File Descriptor: ");
                        int seekFd = scanner.nextInt();
                        scanner.nextLine(); //flush enters

                        System.out.println("Offset: ");
                        long offset = scanner.nextLong();
                        scanner.nextLine(); //flush enter

                        System.out.println("(1) SEEK_SET\n(2) SEEK_CUR\n(3) SEEK_END");
                        System.out.println("Whence: ");
                        int whence = scanner.nextInt();
                        scanner.nextLine(); //flush enter

                        NFS_seek(seekFd, offset, whence);
                    }catch (InputMismatchException e){
                        System.out.println("Invalid input");
                    }
                    break;
                case "c":
                    if(myMiddleware == null){
                        System.out.println("You must initialize your NFS");
                        break;
                    }
                    try {
                        System.out.println("File Descriptor: ");
                        int closeFD = scanner.nextInt();
                        scanner.nextLine(); //flush enters

                        NFS_close(closeFD);
                    }catch (InputMismatchException e){
                        System.out.println("Invalid input");
                    }
                    break;
                case "p":
                    if(myMiddleware == null){
                        System.out.println("You must initialize your NFS");
                        break;
                    }
                    NFS_printFDs();
                    break;
                case "pc":
                    if(myMiddleware == null){
                        System.out.println("You must initialize your NFS");
                        break;
                    }
                    NFS_printCache();
                    break;
                case "d":
                    if(myMiddleware == null){
                        System.out.println("You must initialize your NFS");
                        break;
                    }
                    try {
                        System.out.println("Read File Descriptor: ");
                        int readFD = scanner.nextInt();
                        scanner.nextLine(); //flush enter
                        System.out.println("Bytes count for coping: ");
                        int readLength = scanner.nextInt();
                        scanner.nextLine(); //flush enter
                        System.out.println("Write File Descriptor: ");
                        int writeFD = scanner.nextInt();
                        scanner.nextLine(); //flush enter

                        NFS_demo(readFD, readLength, writeFD);
                    }catch (InputMismatchException e){
                        System.out.println("Invalid input");
                    }
                    break;
                case "m":
                    if(myMiddleware == null){
                        System.out.println("You must initialize your NFS");
                        break;
                    }
                    NFS_metrics();
                    break;
                case "e":
                    endUI = true;
                    break;
                default:
            }
            if(endUI){
                break;
            }
        }
        if(myMiddleware!=null) {
            myMiddleware.Terminate();
            try {
                myMiddleware.join(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void NFS_init(String ipAddress, int port,
            int cacheBlocks, int blockSize, long freshT) throws IOException {
        myMiddleware = new MiddlewareNFS(ipAddress, port, cacheBlocks, blockSize, freshT);
        myMiddleware.start();
    }
    public static void NFS_open(String filename, String flagsInput){
        System.out.println("--------------------------------------------");
        String[] flags = flagsInput.split(",");

        Integer fd = myMiddleware.MiddlewareNFS_open(filename, flags);

        if(fd != -1){
            fileDescriptors.add(fd);
//            fileDescriptorMap.put(fd, filename+", flags: "+flagsInput);
            System.out.println("File Descriptor " +fd +" opened");
        }
        else {
            System.out.println("Open failed");
        }
    }
    public static void NFS_read(int fdId, int readLength){
        System.out.println("--------------------------------------------");
        byte[] readData = myMiddleware.MiddlewareNFS_read(fdId, readLength);

        if(readData==null)
            System.err.println("Read failed");
        else {
            String readString = new String(readData);
            String printable = readString.replaceAll("\\P{Print}", ".");
            if(printable.equals(""))
                System.err.println("No bytes read");
            else
                System.out.println("printable");
        }
    }
    public static void NFS_write(int fdId, String writeData){
        System.out.println("--------------------------------------------");
        System.out.println(myMiddleware.MiddlewareNFS_write(fdId, writeData.getBytes()));
    }
    public static void NFS_seek(int fdId, long offset, int whence){
        System.out.println("--------------------------------------------");
        if(whence<1 || whence>3){
            System.out.println("Invalid whence value");
        }

        Long newPosition = myMiddleware.MiddlewareNFS_seek(fdId, offset, whence);

        if(newPosition != null){
            System.out.println("File Descriptor moved to " + newPosition);
        }
        else {
            System.out.println("File Descriptor doesn't exists");
        }
    }
    public static void NFS_close(Integer fdId){
        System.out.println("--------------------------------------------");
        if(myMiddleware.MiddlewareNFS_close(fdId)){
            fileDescriptors.remove(fdId);
            System.out.println("File Descriptor Closed");
        }
        else {
            fileDescriptors.remove(fdId);
            System.out.println("File Descriptor doesn't exists... removed");
        }
    }
    public static void NFS_demo(int readFD, int readLength, int writeFD){
        System.out.println("--------------------------------------------");
        byte[] readData = myMiddleware.MiddlewareNFS_read(readFD, readLength);

        if(readData==null)
            System.err.println("Read failed");
        else {
            //System.out.println("readData len " + readData.length);
            myMiddleware.MiddlewareNFS_write(writeFD, readData);
        }
    }
    public static void NFS_metrics(){
        NFS_open("a.png","1");
        for(int i=0; i<10; i++){
            System.out.println(i+"*************"+i);
            NFS_read(1,50000);
            NFS_seek(1,0,1);
            myMiddleware.PrintMetrics();
        }
    }
    public static void NFS_printFDs(){
        System.out.println("--------------------------------------------");
        for(Integer fdId : fileDescriptors){
            System.out.println(myMiddleware.MiddlewareNFS_printFD(fdId));
        }
    }
    public static void NFS_printCache(){
        myMiddleware.MiddlewareNFS_printCache();
    }
}

//MIDDLEWARE CLASS
class MiddlewareNFS extends Thread{
    private CacheMemory myCache;
    private final int HEADER_MAX_SIZE = HeaderSize();
    private final int PAYLOAD_MAX_SIZE = 5000;
    private final int PACKET_SIZE = HEADER_MAX_SIZE+PAYLOAD_MAX_SIZE;
    private int BLOCK_SIZE;
    private long FRESH;
    private static final int TIMEOUT = 500;
    InetSocketAddress serverAddress;
    DatagramSocket mySocket;
    private boolean middlewareRun = true;

    private int fdCounter = 0;
    private int openCounter = 0;
    private int readCounter = 0;
    private int writeCounter = 0;

    LinkedList<byte[]> openReplies;
    LinkedList<byte[]> readReplies;
    LinkedList<byte[]> writeReplies;

    LinkedHashMap<String, FileAttributesNFS> fileAttributesMap; //fileName-attributes
    LinkedHashMap<Integer, FileDescriptorNFS> fileDescriptorMap; //fdId-FD

    private ReentrantLock receiveMutex;

    private int bytesOverNetwork = 0;
    private int remoteCalls = 0;
    private int remoteReplies = 0;

    private int HeaderSize(){
        int longLength = String.valueOf(Long.MAX_VALUE).length();
        int intLength = String.valueOf(Integer.MAX_VALUE).length();
        int commas = 5;
        int operations = 5;

        int size = operations + 2*intLength + 2*longLength + commas;
//        System.out.println(size);
        // 2* because UTF-16
        return 2*size;
    }

    public void PrintMetrics(){
        System.out.println("Total remote calls: " + remoteCalls);
        System.out.println("Total remote replies: " + remoteReplies);
        System.out.println("Bytes over the network: " + bytesOverNetwork);
    }

    public MiddlewareNFS(String ipAddress, int port,
                         int cacheBlocks, int blockSize, long freshT) throws IOException {
        mySocket = new DatagramSocket();
        mySocket.setSoTimeout(TIMEOUT);
        serverAddress = new InetSocketAddress(ipAddress, port);

        BLOCK_SIZE = blockSize;
        FRESH = freshT*(1000L);
        myCache = new CacheMemory(cacheBlocks, blockSize, FRESH);

        openReplies = new LinkedList<>();
        readReplies = new LinkedList<>();
        writeReplies = new LinkedList<>();

        fileAttributesMap = new LinkedHashMap<>();
        fileDescriptorMap = new LinkedHashMap<>();

        receiveMutex = new ReentrantLock();
    }

    public void Terminate(){
        middlewareRun = false;
        mySocket.close();
    }

    public void ReceivePacket() throws IOException {
        byte[] bytes = new byte[PACKET_SIZE];

        DatagramPacket rcv_packet = new DatagramPacket(bytes, bytes.length, serverAddress);

        mySocket.receive(rcv_packet);

        int nonNullBytes = rcv_packet.getData().length;
        while (true) {
            if (nonNullBytes-- <= 0 || rcv_packet.getData()[nonNullBytes] != 0) break;
        }

        byte[] payloadBytes = new byte[nonNullBytes+1];
        System.arraycopy(rcv_packet.getData(), 0, payloadBytes, 0, nonNullBytes+1);

        String header = new String(rcv_packet.getData(), 0, nonNullBytes+1);
        String[] partedPayload = header.split(",");

        String operation = partedPayload[0];

        switch (operation){
            case "open":
                openReplies.add(payloadBytes);
                break;
            case "read":
                bytesOverNetwork += payloadBytes.length;
                remoteReplies++;
                readReplies.add(payloadBytes);
                break;
            case "write":
                bytesOverNetwork += payloadBytes.length;
                remoteReplies++;
                writeReplies.add(payloadBytes);
                break;
            default:
        }
    }

    public void SendPacket(byte[] payloadBytes){
        DatagramPacket packet = new DatagramPacket(payloadBytes, payloadBytes.length, serverAddress);

        try {
            mySocket.send(packet);
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void run() {
        while (middlewareRun){
            receiveMutex.lock();
            try {
                ReceivePacket();
            } catch (IOException ignored) {}
            receiveMutex.unlock();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public Integer MiddlewareNFS_open(String fileName, String[] flags){
        if(flags!=null) {
            for (String flag : flags) {
                try {
                    if (Integer.parseInt(flag) < 1 || Integer.parseInt(flag) > 6) {
                        System.out.println("Not valid flag");
                        return -1;
                    }
                } catch (NumberFormatException e) {
                    System.out.println(e.getMessage());
                    return -1;
                }
            }
        }

        //FILE ALREADY OPEN
        if(fileAttributesMap.containsKey(fileName)){
            FileAttributesNFS fileAttributes = fileAttributesMap.get(fileName);

            //REOPEN WITHOUT CREATING FD
            if(flags==null){
                openCounter++;
                String request = "open,"+openCounter+',' + fileName+",1";

                while (true) {

                    receiveMutex.lock();
                    byte[] replyBytes = openReplies.poll();
                    receiveMutex.unlock();

                    if(replyBytes == null){
                        System.out.println(request);
                        SendPacket(request.getBytes());
                        try {
                            Thread.sleep(TIMEOUT);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        continue;
                    }
                    String reply = new String(replyBytes);
                    String[] replyParted = reply.split(",");

                    if(!replyParted[1].equals(String.valueOf(openCounter)))
                    {
                        continue;
                    }

                    if(replyParted[2].equals("failed")){
                        System.out.println(fileName+" open failed");
                        return -1;
                    }

                    Long fileId = Long.valueOf(replyParted[2]);
                    Long fileSize = Long.valueOf(replyParted[3]);
                    boolean canRead = replyParted[4].equals("1");
                    boolean canWrite = replyParted[5].equals("1");
                    Long lastAccessed = System.currentTimeMillis();

                    //UPDATE ALL FILE DESCRIPTORS
                    for (FileDescriptorNFS fd : fileDescriptorMap.values()){
                        if(fd.fileId.equals(fileAttributes.fileId))
                            fd.setFileId(fileId);
                    }

                    fileAttributes.setFileId(fileId);
                    fileAttributes.setFileSize(fileSize);
                    fileAttributes.setCanRead(canRead);
                    fileAttributes.setCanWrite(canWrite);
                    fileAttributes.setLastAccessed(lastAccessed);

                    //openreplies.removeAll()
                    break;
                }
                return 0;
            }
            //CREATE FD
            else {
                List<String> flagList = Arrays.asList(flags);

                boolean writeOnlyFlag = flagList.contains("6");
                boolean readFlag = flagList.contains("1") || flagList.contains("4") || flagList.contains("5");
                boolean fdCanRead = !writeOnlyFlag && readFlag && fileAttributes.canRead;

                boolean readOnlyFlag = flagList.contains("5");
                boolean writeFlag = flagList.contains("1") || flagList.contains("4") || flagList.contains("6");
                boolean fdCanWrite = !readOnlyFlag && writeFlag && fileAttributes.canWrite;

                //files exists
                if (flagList.contains("2")) {
                    System.out.println("O_EXCL: File exists");
                    return -1;
                }
                //if not both permissions confirmed
                if (flagList.contains("4") && !(fdCanRead && fdCanWrite)) {
                    System.out.println("O_RDWR: Either read or write permission denied");
                    return -1;
                }
                //permission denied
                if (flagList.contains("5") && !fdCanRead) {
                    System.out.println("O_RDONLY: Read permission denied");
                    return -1;
                }
                //permission denied
                if (flagList.contains("6") && !fdCanWrite) {
                    System.out.println("O_WRONLY: Write permission denied");
                    return -1;
                }
                //if no permissions at all
                if (!(fdCanRead || fdCanWrite)) {
                    System.out.println("O_CREAT: Both read and write permissions denied");
                    return -1;
                }
                if (flagList.contains("3")) {
                    if (!fdCanWrite) {
                        System.out.println("O_TRUNC: Can not truncate, write permission denied");
                        return -1;
                    }
                    //IF NEED TO TRUNCATE ONLY
                    else {
                        openCounter++;
                        StringBuilder request = new StringBuilder("open,"+openCounter+',' + fileName);

                        for (String flag : flags) {
                            request.append(",").append(flag);
                        }

                        while (true) {
                            receiveMutex.lock();
                            byte[] replyBytes = openReplies.poll();
                            receiveMutex.unlock();

                            if(replyBytes == null){
                                System.out.println(request.toString());
                                SendPacket(request.toString().getBytes());
                                try {
                                    Thread.sleep(TIMEOUT);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                continue;
                            }
                            String reply = new String(replyBytes);
                            String[] replyParted = reply.split(",");

                            if(!replyParted[1].equals(String.valueOf(openCounter)))
                            {
                                continue;
                            }

                            if(replyParted[2].equals("failed")){
                                System.out.println(fileName+" open failed");
                                return -1;
                            }

                            Long fileId = Long.valueOf(replyParted[2]);
                            Long fileSize = Long.valueOf(replyParted[3]);
                            boolean canRead = replyParted[4].equals("1");
                            boolean canWrite = replyParted[5].equals("1");
                            Long lastAccessed = System.currentTimeMillis();

                            fileAttributes.setFileId(fileId);
                            fileAttributes.setFileSize(fileSize);
                            fileAttributes.setCanRead(canRead);
                            fileAttributes.setCanWrite(canWrite);
                            fileAttributes.setLastAccessed(lastAccessed);

                            //openreplies.removeAll()
                            break;
                        }
                    }
                }

                fdCounter++;
                FileDescriptorNFS newFD = new FileDescriptorNFS(fdCounter, fileName, fileAttributes.fileId,
                        0L, fdCanRead, fdCanWrite);

                fileDescriptorMap.put(fdCounter, newFD);
            }
        }
        //FILE NEVER OPENED
        else {
            openCounter++;
            StringBuilder request = new StringBuilder("open,"+openCounter+',' + fileName);

            for (String flag : flags) {
                request.append(",").append(flag);
            }

            while (true) {
                receiveMutex.lock();
                byte[] replyBytes = openReplies.poll();
                receiveMutex.unlock();

                if(replyBytes == null){
                    System.out.println(request.toString());
                    SendPacket(request.toString().getBytes());
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                String reply = new String(replyBytes);
                String[] replyParted = reply.split(",");

                if(!replyParted[1].equals(String.valueOf(openCounter)))
                {
                    continue;
                }

                if(replyParted[2].equals("failed")){
                    System.out.println(fileName+" open failed");
                    return -1;
                }

                Long fileId = Long.valueOf(replyParted[2]);
                Long fileSize = Long.valueOf(replyParted[3]);
                boolean canRead = replyParted[4].equals("1");
                boolean canWrite = replyParted[5].equals("1");
                Long lastAccessed = System.currentTimeMillis();

                FileAttributesNFS fileAttributes=
                        new FileAttributesNFS(fileName, fileId, fileSize, canRead, canWrite, lastAccessed);

                fileAttributesMap.put(fileName,fileAttributes);

                //openreplies.removeAll()
                break;
            }

            FileAttributesNFS fileAttributes = fileAttributesMap.get(fileName);

            List<String> flagList = Arrays.asList(flags);

            boolean writeOnlyFlag = flagList.contains("6");
            boolean readFlag = flagList.contains("1") || flagList.contains("4") || flagList.contains("5");
            boolean fdCanRead = !writeOnlyFlag && readFlag && fileAttributes.canRead;

            boolean readOnlyFlag = flagList.contains("5");
            boolean writeFlag = flagList.contains("1") || flagList.contains("4") || flagList.contains("6");
            boolean fdCanWrite = !readOnlyFlag && writeFlag && fileAttributes.canWrite;

            //if not both permissions confirmed
            if(flagList.contains("4") && !(fdCanRead && fdCanWrite)) {
                System.out.println("O_RDWR: Either read or write permission denied");
                System.out.println("kako den prepei");
                return -1;
            }
            //permission denied
            if(flagList.contains("5") && !fdCanRead){
                System.out.println("O_RDONLY: Read permission denied");
                System.out.println("kako den prepei");
                return -1;
            }
            //permission denied
            if(flagList.contains("6") && !fdCanWrite){
                System.out.println("O_WRONLY: Write permission denied");
                System.out.println("kako den prepei");
                return -1;
            }
            //if no permissions at all
            if(!(fdCanRead || fdCanWrite)) {
                System.out.println("O_CREAT: Both read and write permissions denied");
                System.out.println("kako den prepei");
                return -1;
            }

            fdCounter++;
            FileDescriptorNFS newFD = new FileDescriptorNFS(fdCounter, fileName, fileAttributes.fileId,
                    0L, fdCanRead, fdCanWrite);

            fileDescriptorMap.put(fdCounter,newFD);
        }
        return fdCounter;
    }

    public byte[] MiddlewareNFS_read(Integer fdID, int readLength) {
        if(!fileDescriptorMap.containsKey(fdID)){
            System.out.println("FD doesn't exists");
            return null;
        }
        if(readLength == 0){
            System.out.println("No bytes read");
            return null;
        }

        FileDescriptorNFS fileDescriptor = fileDescriptorMap.get(fdID);
        if(!fileDescriptor.canRead){
            System.out.println("Read permission denied");
            return null;
        }
        long originalPos = fileDescriptor.position;
        int originalReadLength = readLength;


        FileAttributesNFS fileAttributes = fileAttributesMap.get(fileDescriptor.fileName);
        List<byte[]> readPayloadBytesList = new ArrayList<>();

        //IF CACHE COULD HAVE DATA
        if(fileAttributes.isValid()) {
            if(myCache.ContainsFile(fileDescriptor.fileName)) {
                //SEARCH CACHE
                List<long[]> cachedBytesInfo = myCache.GetContainedInfo(fileDescriptor, readLength);

                if(cachedBytesInfo.isEmpty()){
                    System.out.println("Cache Bytes just expired, send to server");
                    fileAttributes.setLastAccessed(0L);
                    return MiddlewareNFS_read(fdID, readLength);
                }

                cachedBytesInfo.sort(new Comparator<long[]>() {
                    @Override
                    public int compare(long[] o1, long[] o2) {
                        return Long.compare(o1[1], o2[1]);
                    }
                });

                //subtract fromCache bytes from payload to ask maybe in loop
                byte[] fromCache = myCache.ReadBytes(cachedBytesInfo);
                long cachedStart = cachedBytesInfo.get(0)[1];
                long cachedEnd = cachedBytesInfo.get(cachedBytesInfo.size() - 1)[2];
                /*System.out.println("cachedStart "+cachedStart
                        +"\ncachedEnd "+cachedEnd);*/
                int beforeLength = (int)(cachedStart - fileDescriptor.position);
                int afterLength = (int)(fileDescriptor.position + (long) readLength - (cachedEnd+1L));

                /*System.out.println("Before len "+beforeLength
                        +"\nfrom cache len "+fromCache.length
                        +"\nafter len " + afterLength);*/
                byte[] finalByteArray = new byte[beforeLength + fromCache.length + afterLength];
                //System.out.println("finalByteArray len " + finalByteArray.length);

                //GET BEFORE BYTES
                if(beforeLength>0) {
                    readLength = beforeLength;
                    readCounter++;
                    int fragmentSeq = 0;
                    int efficientPayloadSize = (BLOCK_SIZE <= PAYLOAD_MAX_SIZE)
                            ? (PAYLOAD_MAX_SIZE / BLOCK_SIZE) * BLOCK_SIZE
                            : PAYLOAD_MAX_SIZE;
                    int blocksNeeded = (readLength % BLOCK_SIZE == 0) ? readLength / BLOCK_SIZE : readLength / BLOCK_SIZE + 1;
                    int fragmentLength = Math.min(blocksNeeded * BLOCK_SIZE, efficientPayloadSize);
                    int totalFragments = (readLength % fragmentLength == 0)
                            ? readLength / fragmentLength
                            : readLength / fragmentLength + 1;

                    while (fragmentSeq != totalFragments) {
                        fragmentSeq++;

                        if (fileDescriptor.position >= fileAttributes.fileSize) {
                            System.out.println("EOF or beyond EOF");
                            break;
                        }

                        //nested loop, maybe in for
                        while (true) {
                            StringBuilder request = new StringBuilder("read," + readCounter);
                            request.append(',').append(fragmentSeq).
                                    append(',').append(fileDescriptor.fileId);

                            //MAYBE READ ONLY BLOCK_SIZE
                            request.append(',').append(fileDescriptor.position).
                                    append(',').append(fragmentLength);

                            receiveMutex.lock();
                            byte[] replyBytes = readReplies.poll();
                            receiveMutex.unlock();

                            //REPLY CHECKS
                            if (replyBytes == null) {
                                System.out.println(request.toString());
                                SendPacket(request.toString().getBytes());
                                remoteCalls++;
                                try {
                                    Thread.sleep(TIMEOUT);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                continue;
                            }

                            String reply = new String(replyBytes);
                            String[] replyParted = reply.split(",");

                            if (!replyParted[1].equals(String.valueOf(readCounter))) {
                                continue;
                            }

                            if (!replyParted[2].equals(String.valueOf(fragmentSeq))) {
                                continue;
                            }

                            if (replyParted[3].equals("error")) {
                                System.out.println("Server error");
                                return null;
                            }
                            if (replyParted[3].equals("failed")) {
                                System.out.println("Server failed");
                                return null;
                            }
                            if (replyParted[3].equals("closed")) {
                                receiveMutex.lock();
                                while (!readReplies.isEmpty()) {
                                    readReplies.removeFirst();
                                }
                                receiveMutex.unlock();
                                int success = MiddlewareNFS_open(fileDescriptor.fileName, null);
                                if (success == 0) {
                                    continue;
                                } else {
                                    System.out.println("Failed at re-opening");
                                    return null;
                                }
                            }

                            //MAIN FUNCTIONALITY
                            Long fileSize = Long.parseLong(replyParted[3]);

                            int offset = 0;
                            for (int i = 0; i < 4; i++) {
                                offset += replyParted[i].length() + 1;
                            }

                            byte[] payloadBytes = Arrays.copyOfRange(replyBytes, offset, replyBytes.length);
                            readPayloadBytesList.add(payloadBytes);

                            fileAttributes.setLastAccessed(System.currentTimeMillis());
                            fileAttributes.setFileSize(fileSize);

                            long movedPosition = payloadBytes.length;
                            fileDescriptor.setPosition(fileDescriptor.position + movedPosition);

                            //readReplies.removeAll()
                            break;
                        }
                    }

                    int totalLength = 0;
                    for (byte[] fragment : readPayloadBytesList) {
                        totalLength += fragment.length;
                    }
                    byte[] beforeCacheBytes = new byte[totalLength];
                    int destPos = 0;
                    for (byte[] fragment : readPayloadBytesList) {
                        System.arraycopy(fragment, 0, beforeCacheBytes, destPos, fragment.length);
                        destPos += fragment.length;
                    }

                    System.arraycopy(beforeCacheBytes,0, finalByteArray, 0, beforeLength);
                }
                //END OF BEFORE CACHE FUNCTIONALITY

                System.arraycopy(fromCache,0, finalByteArray, beforeLength, fromCache.length);
                readPayloadBytesList = new ArrayList<>();
                fileDescriptor.setPosition(cachedEnd+1L);

                //GET AFTER CACHE BYTES
                if(afterLength>0) {
                    readLength = afterLength;
                    readCounter++;
                    int fragmentSeq = 0;
                    int efficientPayloadSize = (BLOCK_SIZE <= PAYLOAD_MAX_SIZE)
                            ? (PAYLOAD_MAX_SIZE / BLOCK_SIZE) * BLOCK_SIZE
                            : PAYLOAD_MAX_SIZE;
                    int blocksNeeded = (readLength % BLOCK_SIZE == 0) ? readLength / BLOCK_SIZE : readLength / BLOCK_SIZE + 1;
                    int fragmentLength = Math.min(blocksNeeded * BLOCK_SIZE, efficientPayloadSize);
                    int totalFragments = (readLength % fragmentLength == 0)
                            ? readLength / fragmentLength
                            : readLength / fragmentLength + 1;

                    while (fragmentSeq != totalFragments) {
                        fragmentSeq++;

                        if (fileDescriptor.position >= fileAttributes.fileSize) {
                            System.out.println("EOF or beyond EOF");
                            break;
                        }

                        //nested loop, maybe in for
                        while (true) {
                            StringBuilder request = new StringBuilder("read," + readCounter);
                            request.append(',').append(fragmentSeq).
                                    append(',').append(fileDescriptor.fileId);

                            //MAYBE READ ONLY BLOCK_SIZE
                            request.append(',').append(fileDescriptor.position).
                                    append(',').append(fragmentLength);

                            receiveMutex.lock();
                            byte[] replyBytes = readReplies.poll();
                            receiveMutex.unlock();

                            //REPLY CHECKS
                            if (replyBytes == null) {
                                System.out.println(request.toString());
                                SendPacket(request.toString().getBytes());
                                remoteCalls++;
                                try {
                                    Thread.sleep(TIMEOUT);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                continue;
                            }

                            String reply = new String(replyBytes);
                            String[] replyParted = reply.split(",");

                            if (!replyParted[1].equals(String.valueOf(readCounter))) {
                                continue;
                            }

                            if (!replyParted[2].equals(String.valueOf(fragmentSeq))) {
                                continue;
                            }

                            if (replyParted[3].equals("error")) {
                                System.out.println("Server error");
                                return null;
                            }
                            if (replyParted[3].equals("failed")) {
                                System.out.println("Server failed");
                                return null;
                            }
                            if (replyParted[3].equals("closed")) {
                                receiveMutex.lock();
                                while (!readReplies.isEmpty()) {
                                    readReplies.removeFirst();
                                }
                                receiveMutex.unlock();
                                int success = MiddlewareNFS_open(fileDescriptor.fileName, null);
                                if (success == 0) {
                                    continue;
                                } else {
                                    System.out.println("Failed at re-opening");
                                    return null;
                                }
                            }

                            //MAIN FUNCTIONALITY
                            Long fileSize = Long.parseLong(replyParted[3]);


                            int offset = 0;
                            for (int i = 0; i < 4; i++) {
                                offset += replyParted[i].length() + 1;
                            }

                            byte[] payloadBytes = Arrays.copyOfRange(replyBytes, offset, replyBytes.length);
                            readPayloadBytesList.add(payloadBytes);

                            fileAttributes.setLastAccessed(System.currentTimeMillis());
                            fileAttributes.setFileSize(fileSize);

                            long movedPosition = payloadBytes.length;
                            fileDescriptor.setPosition(fileDescriptor.position + movedPosition);

                            //readReplies.removeAll()
                            break;
                        }
                    }

                    int totalLength = 0;
                    for (byte[] fragment : readPayloadBytesList) {
                        totalLength += fragment.length;
                    }
                    byte[] afterCacheBytes = new byte[totalLength];
                    int destPos = 0;
                    for (byte[] fragment : readPayloadBytesList) {
                        System.arraycopy(fragment, 0, afterCacheBytes, destPos, fragment.length);
                        destPos += fragment.length;
                    }

                    int afterReadLength = Math.min(afterCacheBytes.length, afterLength);
                    System.arraycopy(afterCacheBytes,
                            0, finalByteArray,
                            beforeLength+fromCache.length, afterReadLength);
                }
                //END OF AFTER CACHE FUNCTIONALITY

                int nonNullBytes = finalByteArray.length;
                while (true) {
                    if (nonNullBytes-- <= 0 || finalByteArray[nonNullBytes] != 0) break;
                }

                byte[] forCacheBytes = new byte[nonNullBytes+1];
                System.arraycopy(finalByteArray, 0, forCacheBytes, 0, nonNullBytes+1);

                //System.out.println("forCacheBytes len " + forCacheBytes.length);

                int minRead = Math.min(originalReadLength, forCacheBytes.length);

                //copy to total bytes to cache
                fileDescriptor.setPosition(originalPos);
                myCache.WriteBytes(fileDescriptor, forCacheBytes);

                fileDescriptor.setPosition(originalPos + (long) minRead);

                //System.out.println("forApp len " + minRead);

                return Arrays.copyOfRange(forCacheBytes,0,minRead);
                //END OF CACHE FUNCTIONALITY
            }
        }

        readCounter++;
        int fragmentSeq = 0;
        int efficientPayloadSize = (BLOCK_SIZE <= PAYLOAD_MAX_SIZE)
                ? (PAYLOAD_MAX_SIZE/BLOCK_SIZE)*BLOCK_SIZE
                : PAYLOAD_MAX_SIZE;
        int blocksNeeded = (readLength%BLOCK_SIZE==0) ? readLength/BLOCK_SIZE : readLength/BLOCK_SIZE + 1;
        int fragmentLength = Math.min(blocksNeeded*BLOCK_SIZE,efficientPayloadSize);
        int totalFragments = (readLength%fragmentLength==0)
                ? readLength/fragmentLength
                : readLength/fragmentLength +1;

        while (fragmentSeq != totalFragments) {
            fragmentSeq++;

            if(fileDescriptor.position >= fileAttributes.fileSize) {
                System.out.println("EOF or beyond EOF");
                break;
            }

            while (true) {
                StringBuilder request = new StringBuilder("read,"+readCounter);
                request.append(',').append(fragmentSeq).
                        append(',').append(fileDescriptor.fileId);

                //MAYBE READ ONLY BLOCK_SIZE
                request.append(',').append(fileDescriptor.position).
                        append(',').append(fragmentLength);

                receiveMutex.lock();
                byte[] replyBytes = readReplies.poll();
                receiveMutex.unlock();

                //REPLY CHECKS
                if (replyBytes == null) {
                    System.out.println(request.toString());
                    SendPacket(request.toString().getBytes());
                    remoteCalls++;
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                String reply = new String(replyBytes);
                String[] replyParted = reply.split(",");

                if (!replyParted[1].equals(String.valueOf(readCounter))) {
                    continue;
                }

                if (!replyParted[2].equals(String.valueOf(fragmentSeq))) {
                    continue;
                }

                if (replyParted[3].equals("error")) {
                    System.out.println("Server error");
                    return null;
                }
                if (replyParted[3].equals("failed")) {
                    System.out.println("Server failed");
                    return null;
                }
                if (replyParted[3].equals("closed")) {
                    receiveMutex.lock();
                    while (!readReplies.isEmpty()) {
                        readReplies.removeFirst();
                    }
                    receiveMutex.unlock();
                    int success = MiddlewareNFS_open(fileDescriptor.fileName, null);
                    if(success == 0){
                        continue;
                    }else {
                        System.out.println("Failed at re-opening");
                        return null;
                    }
                }

                //MAIN FUNCTIONALITY
                Long fileSize = Long.parseLong(replyParted[3]);
                //PERMISSIONS
                /*boolean canRead = replyParted[4].equals("1");
                boolean canWrite = replyParted[5].equals("1");*/



                int offset = 0;
                for (int i = 0; i < 4; i++) {
                    offset += replyParted[i].length() + 1;
                }

                byte[] payloadBytes = Arrays.copyOfRange(replyBytes,offset,replyBytes.length);
                readPayloadBytesList.add(payloadBytes);

                fileAttributes.setLastAccessed(System.currentTimeMillis());
                fileAttributes.setFileSize(fileSize);

                long movedPosition = payloadBytes.length;
                fileDescriptor.setPosition(fileDescriptor.position + movedPosition);

                //readReplies.removeAll()
                break;
            }
        }

        int totalLength = 0;
        for(byte[] fragment : readPayloadBytesList){
            totalLength += fragment.length;
        }
        byte[] forCache = new byte[totalLength];
        int destPos = 0;
        for(byte[] fragment : readPayloadBytesList){
            System.arraycopy(fragment, 0, forCache, destPos, fragment.length);
            destPos += fragment.length;
        }

        int minRead = Math.min(readLength, forCache.length);

        //copy to total bytes to cache
        fileDescriptor.setPosition(originalPos);
        myCache.WriteBytes(fileDescriptor, forCache);

        fileDescriptor.setPosition(originalPos + (long)minRead);
        byte[] forApp = Arrays.copyOfRange(forCache,0,minRead);

        return forApp;
    }

    public String MiddlewareNFS_write(Integer fdID, byte[] writeData){
        if(!fileDescriptorMap.containsKey(fdID)){
            return "FD doesn't exists";
        }

        Long totalWritten = 0L;

        FileDescriptorNFS fileDescriptor = fileDescriptorMap.get(fdID);
        long originalPos = fileDescriptor.position;

        if(!fileDescriptor.canWrite){
            return "Write permission denied";
        }
        FileAttributesNFS fileAttributes = fileAttributesMap.get(fileDescriptor.fileName);

        int writeSize = writeData.length;

        writeCounter++;

        int fragmentSeq = 0;
        int fragments = (writeSize%PAYLOAD_MAX_SIZE==0)
                ? writeSize/PAYLOAD_MAX_SIZE
                : writeSize/PAYLOAD_MAX_SIZE +1;
        //MAYBE REPLACE WITH BLOCK_SIZE
        int writeSizeMod = (writeSize%PAYLOAD_MAX_SIZE==0) ? PAYLOAD_MAX_SIZE : writeSize%PAYLOAD_MAX_SIZE;

        int srcPos = 0;
        while (fragmentSeq != fragments) {
            fragmentSeq++;
            byte[] splitData;
            if(fragmentSeq==fragments){
                splitData = Arrays.copyOfRange(writeData, srcPos, writeSize);
                srcPos += writeSizeMod;
            }else {
                splitData = Arrays.copyOfRange(writeData, srcPos, srcPos+PAYLOAD_MAX_SIZE);
                srcPos = fragmentSeq * PAYLOAD_MAX_SIZE;
            }

            while (true) {
                //here in case of re-open for new fileId
                StringBuilder request = new StringBuilder("write," + writeCounter);
                request.append(',').append(fragmentSeq)
                        .append(',').append(fileDescriptor.fileId);

                request.append(',').append(fileDescriptor.position)
                        .append(',');

                byte[] header = request.toString().getBytes();
                byte[] packet = new byte[header.length + splitData.length];

                System.arraycopy(header, 0, packet, 0, header.length);
                System.arraycopy(splitData, 0, packet, header.length, splitData.length);


                receiveMutex.lock();
                byte[] replyBytes = writeReplies.poll();
                receiveMutex.unlock();

                if (replyBytes == null) {
                    System.out.println(request.toString());
                    SendPacket(packet);
                    remoteCalls++;
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                String reply = new String(replyBytes);
                String[] replyParted = reply.split(",");

                if (!replyParted[1].equals(String.valueOf(writeCounter))) {
                    continue;
                }

                if (!replyParted[2].equals(String.valueOf(fragmentSeq))) {
                    continue;
                }

                if (replyParted[3].equals("error")) {
                    return "error";
                }
                if (replyParted[3].equals("failed")) {
                    return "failed";
                }
                if (replyParted[3].equals("closed")) {
                    receiveMutex.lock();
                    while (!readReplies.isEmpty()) {
                        writeReplies.removeFirst();
                    }
                    receiveMutex.unlock();
                    int success = MiddlewareNFS_open(fileDescriptor.fileName, null);
                    if(success == 0){
                        continue;
                    }else {
                        return "failed at write reopening";
                    }
                }

                Long fileSize = Long.parseLong(replyParted[3]);
                //PERMISSIONS
            /*boolean canRead = replyParted[4].equals("1");
            boolean canWrite = replyParted[5].equals("1");*/


                fileAttributes.setFileSize(fileSize);
                fileAttributes.setLastAccessed(System.currentTimeMillis());

                long bytesWritten = Long.parseLong(replyParted[4]);
                fileDescriptor.setPosition(fileDescriptor.position + bytesWritten);

                totalWritten += bytesWritten;
                break;
            }
        }

        byte[] forCache = new byte[totalWritten.intValue()];
        System.arraycopy(writeData,0,forCache,0, totalWritten.intValue());

        //copy to cache
        fileDescriptor.setPosition(originalPos);
        myCache.WriteBytes(fileDescriptor, forCache);
        fileDescriptor.setPosition(originalPos + totalWritten);

        return "Bytes Written: "+ totalWritten;
    }

    public Long MiddlewareNFS_seek(Integer fdID, Long offset, int whence){
        if(!fileDescriptorMap.containsKey(fdID)){
            return null;
        }
        Long newPosition = null;

        FileDescriptorNFS fileDescriptor = fileDescriptorMap.get(fdID);

        FileAttributesNFS fileAttributes = fileAttributesMap.get(fileDescriptor.fileName);

        switch (whence){
            //SEEK_SET
            case 1:
                newPosition = offset;
                break;
            //SEEK_CUR
            case 2:
                newPosition = fileDescriptor.position + offset;
                break;
            //SEEK_END
            case 3:
                newPosition = fileAttributes.fileSize - offset;
                break;
        }

        fileDescriptor.setPosition(newPosition);

        return newPosition;
    }

    public boolean MiddlewareNFS_close(Integer fdID){
        if(!fileDescriptorMap.containsKey(fdID)){
            return false;
        }

        FileDescriptorNFS fileDescriptor = fileDescriptorMap.remove(fdID);

        return true;
    }

    public String MiddlewareNFS_printFD(int fdID){
        FileDescriptorNFS fileDescriptor = fileDescriptorMap.get(fdID);

        return fileDescriptor.toString();
    }

    public void MiddlewareNFS_printCache(){
        myCache.PrintCache();
    }

    //CUSTOM FILE DESCRIPTOR
    private class FileDescriptorNFS {
        private int fileDescriptorId;
        private String fileName;
        private Long fileId;
        private Long position;
        private boolean canRead;
        private boolean canWrite;

        public FileDescriptorNFS(int fileDescriptorId, String fileName, Long fileId, Long position, boolean canRead, boolean canWrite) {
            this.fileDescriptorId = fileDescriptorId;
            this.fileName = fileName;
            this.fileId = fileId;
            this.position = position;
            this.canRead = canRead;
            this.canWrite = canWrite;
        }

        @Override
        public String toString() {
            return "FD: "+fileDescriptorId+ ", pos: "+position+
                    ", canRead: "+canRead+", canWrite: "+canWrite+
                    ", file: "+fileName;
        }

        public void setFileId(Long fileId) {
            this.fileId = fileId;
        }

        public void setPosition(Long position) {
            this.position = position;
        }

        /*public int getFileDescriptorId() {
            return fileDescriptorId;
        }

        public Long getFileId() {
            return fileId;
        }

        public Long getPosition() {
            return position;
        }

        public boolean isCanRead() {
            return canRead;
        }

        public void setCanRead(boolean canRead) {
            this.canRead = canRead;
        }

        public boolean isCanWrite() {
            return canWrite;
        }

        public void setCanWrite(boolean canWrite) {
            this.canWrite = canWrite;
        }*/
    }

    //CUSTOM FILE ATTRIBUTES
    private class FileAttributesNFS {
        private String fileName;
        private Long fileId;
        private Long fileSize;
        private boolean canRead;
        private boolean canWrite;
        private Long lastAccessed;

        public FileAttributesNFS(String fileName, Long fileId, Long size,
                                 boolean canRead, boolean canWrite, Long lastAccessed) {
            this.fileName = fileName;
            this.fileId = fileId;
            this.fileSize = size;
            this.canRead = canRead;
            this.canWrite = canWrite;
            this.lastAccessed = lastAccessed;
        }

        public void setFileId(Long fileId) {
            this.fileId = fileId;
        }

        public void setFileSize(Long fileSize) {
            this.fileSize = fileSize;
        }

        public void setCanRead(boolean canRead) {
            this.canRead = canRead;
        }

        public void setCanWrite(boolean canWrite) {
            this.canWrite = canWrite;
        }

        public void setLastAccessed(Long lastAccessed) {
            this.lastAccessed = lastAccessed;
        }

        public boolean isValid(){
            return System.currentTimeMillis() < lastAccessed + FRESH;
        }

       /* public String getFileName() {
            return fileName;
        }

        public Long getFileId() {
            return fileId;
        }

        public Long getFileSize() {
            return fileSize;
        }

        public boolean isCanRead() {
            return canRead;
        }

        public boolean isCanWrite() {
            return canWrite;
        }

        public Long getLastAccessed() {
            return lastAccessed;
        }*/
    }

    //CUSTOM CACHE MEMORY
    private class CacheMemory{
        int cacheBlocks;
        int blockSize;
        int totalSize;
        long freshT;

        ArrayList<Block> blockList;
        ArrayList<String> filesContained;

        public CacheMemory(int cacheBlocks, int blockSize, long freshT){
            this.cacheBlocks = cacheBlocks;
            this.blockSize = blockSize;
            totalSize = cacheBlocks*blockSize;
            this.freshT = freshT;

            blockList = new ArrayList<>();
            for (int i=0; i<cacheBlocks; i++){
                blockList.add(new Block(blockSize, freshT));
            }

            filesContained = new ArrayList<>();
        }

        public List<long[]> GetContainedInfo(FileDescriptorNFS fd, int readLength){
            List<long[]> containedInfo = new ArrayList<>();
            for(Block block : blockList){
                if(block.fileName.equals(fd.fileName)) {
                    if (block.isValid()) {
                        long maxStartPos = Math.max(fd.position, block.startPos);
                        long minEndPos = Math.min(fd.position + readLength - 1, block.endPos);
                        if (maxStartPos <= minEndPos) {
                            /*System.out.println("Contained Valid Block " + (blockList.indexOf(block) + 1)
                                    + " start " + maxStartPos
                                    + " end " + minEndPos);*/
                            containedInfo.add(new long[]{blockList.indexOf(block), maxStartPos, minEndPos});
                        }
                    }
                }
            }
            return containedInfo;
        }

        public byte[] ReadBytes(List<long[]> cachedBlocks){
            if(cachedBlocks.isEmpty())
                return null;

            List<byte[]> cachedBytesList = new ArrayList<>();

            for (long[] cachedBlock : cachedBlocks){
                int blockIndex = (int) cachedBlock[0];
                long startPos = cachedBlock[1];
                long endPos = cachedBlock[2];
                Block block = blockList.get(blockIndex);
                cachedBytesList.add(block.GetBytes(startPos, endPos));
            }

            int totalLength = 0;
            for(byte[] fragment : cachedBytesList){
                totalLength += fragment.length;
            }
            byte[] cachedBytes = new byte[totalLength];
            int destPos = 0;
            for(byte[] fragment : cachedBytesList){
                System.arraycopy(fragment, 0, cachedBytes, destPos, fragment.length);
                destPos += fragment.length;
            }

            return cachedBytes;
        }

        public void WriteBytes(FileDescriptorNFS fd, byte[] totalBytes) {
            List<byte[]> blocksForImport = ConstructFragmentedList(totalBytes);

            //to give recentlyAccessed after writing
            List<Block> blocksAdded = new ArrayList<>();

            long startPos = fd.position;
            boolean skipBytes = false;
            for(byte[] bytes : blocksForImport) {
                //System.out.println("Bytes index " + blocksForImport.indexOf(bytes));
                //System.out.println("Bytes for cache " + Arrays.toString(bytes));

                //IF VALID BLOCK CAN BE OVERWRITTEN WITH SAME FILE
                for(Block block : blockList){
                    if(block.isValid()) {
                        if(block.fileName.equals(fd.fileName)) {
                            long maxStartPos = Math.max(startPos, block.startPos);
                            long minEndPos = Math.min(startPos + bytes.length - 1, block.endPos);
                            if (maxStartPos <= minEndPos) {
                                System.out.println("Valid block for overwrite " + blockList.indexOf(block));
                                //System.out.println("Valid bytes " + Arrays.toString(block.GetBytes()));
                                long endPos = startPos + (long) bytes.length - 1L;
                                filesContained.remove(block.fileName);
                                //block.Empty();
                                block.SetFileName(fd.fileName);
                                filesContained.add(fd.fileName);
                                block.SetBytes(bytes);
                                block.SetStartEnd(startPos, endPos);
                                //block.SetLastAccess(System.currentTimeMillis());
                                startPos = endPos + 1L;
                                skipBytes = true;
                                block.SetRecentlyAccessed(true);
                                blocksAdded.add(block);
                                break;
                            }
                        }
                    }
                }

                if(skipBytes){
                    skipBytes = false;
                    continue;
                }

                //REPLACE NON VALID BLOCKS
                for (Block block : blockList) {
                    //System.out.println("Not skipped Block " + (blockList.indexOf(block) + 1));

                    if (!block.isValid()) {
                        System.out.println("not valid Block " + (blockList.indexOf(block) + 1));
                        long endPos = startPos + (long) bytes.length - 1L;
                        filesContained.remove(block.fileName);
                        block.Empty();
                        block.SetFileName(fd.fileName);
                        filesContained.add(fd.fileName);
                        block.SetBytes(bytes);
                        block.SetStartEnd(startPos, endPos);
                        block.SetLastAccess(System.currentTimeMillis());
                        startPos = endPos +1L;
                        skipBytes = true;
                        block.SetRecentlyAccessed(true);
                        blocksAdded.add(block);
                        break;
                    }
                }

                if(skipBytes){
                    skipBytes = false;
                    continue;
                }

                //IF CACHE FULL AND ALL BLOCKS VALID
                //REMOVE THOSE WITH RECENTLY ACCESSED FALSE
                for (Block block : blockList) {
                    //System.out.println("Valid Block " + (blockList.indexOf(block) + 1));

                    if (!block.isRecentlyAccessed()) {
                        System.out.println("not Recent Block " + (blockList.indexOf(block) + 1));
                        long endPos = startPos + (long) bytes.length - 1L;
                        filesContained.remove(block.fileName);
                        block.Empty();
                        block.SetFileName(fd.fileName);
                        filesContained.add(fd.fileName);
                        block.SetBytes(bytes);
                        block.SetStartEnd(startPos, endPos);
                        block.SetLastAccess(System.currentTimeMillis());
                        startPos = endPos +1L;
                        block.SetRecentlyAccessed(true);
                        blocksAdded.add(block);
                        break;
                    }
                }
            }

            for(Block block : blockList){
                block.SetRecentlyAccessed(blocksAdded.contains(block));
            }
        }

        private List<byte[]> ConstructFragmentedList(byte[] forCache){
            int bytesForImport = Math.min(forCache.length, totalSize);

            int fragments = (bytesForImport%blockSize == 0)
                    ? bytesForImport/blockSize
                    : bytesForImport/blockSize + 1;
            int lastFrag = (bytesForImport%blockSize == 0)
                    ? blockSize
                    : bytesForImport%blockSize;

            List<byte[]> forImport = new ArrayList<>();
            for(int i=0; i<fragments; i++){
                if(i == fragments-1){
                    forImport.add(Arrays.copyOfRange(forCache, i*blockSize, i*blockSize+lastFrag));
                    continue;
                }
                forImport.add(Arrays.copyOfRange(forCache, i*blockSize, (i+1)*blockSize));
            }

            return forImport;
        }

        public boolean ContainsFile(String fileName){
            return filesContained.contains(fileName);
        }

        public void PrintCache(){
            for(Block block : blockList){
                System.out.println((blockList.indexOf(block) + 1)+ " " + block.toString());
            }
        }

        //CUSTOM CACHE BLOCK
        private class Block{
            String fileName = "";
            byte[] bytes;
            long startPos = 0L;
            long endPos = 0L;
            long lastAccess = 0L;
            long lastModification = 0L;
            boolean recentlyAccessed = false;
            private long freshT;
            private int blockSize;

            public Block(int blockSize, long freshT) {
                bytes = new byte[blockSize];

                this.blockSize = blockSize;
                this.freshT = freshT;
            }

            public void SetFileName(String fileName) {
                this.fileName = fileName;
            }

            void Empty(){
                fileName = "";
                bytes = new byte[blockSize];
                startPos = 0L;
                endPos = 0L;
                lastAccess = 0L;
                lastModification = 0L;
                recentlyAccessed = false;
            }

            boolean isRecentlyAccessed(){
                return recentlyAccessed;
            }

            void SetRecentlyAccessed(boolean recentlyAccessed){
                this.recentlyAccessed = recentlyAccessed;
            }

            boolean isValid(){
//                System.out.println("isValid " + System.currentTimeMillis() + "\nlAccess " + (lastAccess+freshT));
                return lastAccess!=0L && System.currentTimeMillis() < lastAccess + freshT;
            }

            void SetLastAccess(long lastAccess){
                this.lastAccess = lastAccess;
            }

            void SetStartEnd(long startPos, long endPos){
                this.startPos = startPos;
                this.endPos = endPos;
            }

            void SetBytes(byte[] newBytes){
                System.arraycopy(newBytes, 0, bytes, 0, Math.min(bytes.length, newBytes.length));
            }

            public byte[] GetBytes(long start, long end) {
                int length = (int)(end-start)+1;

                byte[] returnBytes = new byte[length];
                System.arraycopy(bytes,(int)(start-startPos), returnBytes, 0, length);

                return returnBytes;
            }

            byte[] GetBytes(){
                return bytes;
            }

            @Override
            public String toString() {
                return "Block{" +
                        "fileName='" + fileName + '\'' +
                        /*", bytes=" + Arrays.toString(bytes) +*/
                        /*", bytes=" + new String(bytes) +*/
                        ", startPos=" + startPos +
                        ", endPos=" + endPos +
                        ", lastAccess=" + lastAccess +
                        ", is Valid= " + isValid() +
                        ", is Recent= " + recentlyAccessed +
                        /*", timesAccessed=" + lastModification +*/
                        '}';
            }

            /*byte[] GetBytes(int start, int end){
                int length = end-start+1;

                byte[] returnBytes = new byte[length];
                System.arraycopy(bytes,start, returnBytes, 0, length);

                return returnBytes;
            }

            public String GetFileName() {
                return fileName;
            }*/
        }
    }
}
