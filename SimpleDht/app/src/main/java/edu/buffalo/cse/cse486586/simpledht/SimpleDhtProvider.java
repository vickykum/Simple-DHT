package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.widget.TextView;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    private ContentResolver mContentResolver;
    private Uri mUri;
    private int msgKey = 0;
    private String localPort;
    private String localHash;
    private ArrayList<String> localMsgKey = new ArrayList<String>();
    private HashMap<String,String> waitingQuery = new HashMap<String,String>();
    private boolean wait = true;
    private TreeMap<String,String> allPortHash = new TreeMap<String, String>();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        if(selection.equals("*")){
            deleteAll();
            for(Map.Entry<String,String> m:  allPortHash.entrySet()){
                String portToDelete = m.getValue();
                String msgToSend = selection+"|"+localPort+"|"+"DELETE";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portToDelete);
            }
        }
        else if(selection.equals("@")){
            deleteAll();
        }
        else{
            String portToDelete = bestPort(selection);
            if(portToDelete.equals(localPort)){
                getContext().deleteFile(selection);
                localMsgKey.remove(selection);
                Log.v("delete","deleting Successful for "+selection);
            }
            else{
                Log.v("delete","sending to for deleting key "+selection);
                String msgToSend = selection+"|"+localPort+"|"+"DELETE";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portToDelete);
            }
        }
        return 0;
    }

    public void deleteAll(){
        for(int i = 0; i<localMsgKey.size(); i++){
            String filename = localMsgKey.get(i);
            getContext().deleteFile(localMsgKey.get(i));
            Log.v("deleteAll","deleting successfull "+filename);
        }
        localMsgKey = new ArrayList<String>();
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String k = "";
        String v = "";
        Set<Map.Entry<String, Object>> allVals = values.valueSet();
        for(Map.Entry<String,Object> ent: allVals){
            String elem = ent.getKey();
            Log.v("insert","here getkey is "+elem);
            if(elem.equals("key")){
                k = ent.getValue().toString();
            }
            else{
                v = ent.getValue().toString();
            }
        }

        String portToSave = bestPort(k);
        if(portToSave.equals(localPort)){
            Log.v("insert","here in local port for key "+k);
            finalInsert(k,v);
        }
        else{
            Log.v("insert","sending to another port for key "+k);
            String msgToSend = k+"|"+v+"|"+"INSERT";
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portToSave);
        }
        return uri;
    }

    public void finalInsert(String key, String value){
        String filename = key;
        String strTowrite = value + "\n";
        localMsgKey.add(key);
        FileOutputStream outputStream;
        try {
            outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(strTowrite.getBytes());
            outputStream.close();
            Log.v("insert","key is "+key+" Value is "+value+" and filename "+filename);
        } catch (Exception e) {
            Log.e("insert writing", "File write failed");
        }
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        /*My code begins */
        Log.v("Content Provider","I am being visited Content onCreate");

        mContentResolver = getContext().getContentResolver();
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority("edu.buffalo.cse.cse486586.simpledht.provider");
        uriBuilder.scheme("content");
        mUri = uriBuilder.build();

        /*Hack by Steve ko for creating AVD connection
         */
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.v("in Provider",myPort);
        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Log.v(TAG, "Next line with issue");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {

            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        /*Sending off the message that avd -myPort- is live*/
        try {
            String portHash = genHash(portStr);
            localPort = myPort;
            localHash = portHash;
            allPortHash.put(portHash,myPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, portHash, myPort);
        } catch(NoSuchAlgorithmException nsae){
            Log.v("on create","No such algo error");
        }

        return false;
    }

    public String bestPort(String k){
        try{
            String key = genHash(k);
            String first = "";
            int i = 0;
            for(Map.Entry<String,String> port : allPortHash.entrySet()){
                Log.v("bestPort","hash: "+port.getKey()+" port: "+port.getValue());
                if(i == 0){
                    first = port.getValue();
                }
                if(key.compareTo(port.getKey()) < 0){
                    return port.getValue();
                }
                i++;
            }
            return first;
        } catch(NoSuchAlgorithmException nsae){
            Log.v("bestPort","NSAE exception");
            return null;
        }
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor mc = null;
        String filename = selection;
        Log.v("selection", "File name is "+selection);
        if(selection.equals("*")){
            mc = new MatrixCursor(new String[]{"key","value"});
            queryAll(mc);
            for(Map.Entry<String,String> m:  allPortHash.entrySet()){
                String portToDelete = m.getValue();
                String msgToSend = selection+"|"+localPort+"|"+"QUERY";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portToDelete);

                while(wait){
                    //Wait for all the query to be received
                }
                for(Map.Entry<String,String> me : waitingQuery.entrySet()){
                    mc.addRow(new String[]{me.getKey(),me.getValue()});
                }
                //Removed all the saved values from it
                wait = true;
                waitingQuery = new HashMap<String, String>();

            }
        }
        else if(selection.equals("@")){
            mc = new MatrixCursor(new String[]{"key","value"});
            queryAll(mc);
        }
        else{
            String portToQuery = bestPort(selection);
            if(portToQuery.equals(localPort)){
                String val = finalQuery(selection);
                mc = new MatrixCursor(new String[]{"key","value"});
                String data[] = new String[2];
                data[0] = filename;
                data[1] = val;
                mc.addRow(data);
                Log.v("query", "from key "+filename);
                Log.v("query", "value "+val);
                Log.v("query","message retrived successfully");
            }
            else{
                Log.v("query","asking port "+portToQuery+" for value of "+selection);
                String msgToSend = selection+"|"+localPort+"|"+"QUERY";
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, portToQuery);
                String value;
                while(waitingQuery.get(selection) == null){
                    //Wait for the query to be received
                    //Log.v("query","Looping.......");
                }
                value = waitingQuery.get(selection);
                waitingQuery.remove(selection);
                mc = new MatrixCursor(new String[]{"key","value"});
                String data[] = new String[2];
                data[0] = filename;
                data[1] = value;
                mc.addRow(data);
                Log.v("query","Done querying from port "+portToQuery);
            }
        }

        return mc;
    }

    public String finalQuery(String filename){
        String val = null;
        try {
            FileInputStream inputStream;
            inputStream = getContext().openFileInput(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            val = br.readLine();
            br.close();
            inputStream.close();
        } catch (IOException e) {
            Log.e("in query", "File reading failed");
            e.printStackTrace();
        }
        return val;
    }

    public String queryAll(MatrixCursor mc){
        String retval = "";
        if(localMsgKey.isEmpty()){
            return retval;
        }
        for(int i = 0; i<localMsgKey.size(); i++){
            String[] data = new String[]{localMsgKey.get(i),finalQuery(localMsgKey.get(i))};
            if(mc == null){
                retval += data[0]+"&"+data[1];
                if(i < localMsgKey.size() -1 ){
                    retval += "$";
                }
            }
            else{
                mc.addRow(data);
            }
        }
        return retval;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    /*Server Task is also AsyncTask*/
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.v("doInBackgroung","VISITED");
            /*Send to onProgressUpdate().*/
            Log.v("while loop","VISITED");
            ServerSocket serverSocket = sockets[0];
            try {
                while(true){
                    Socket sS = serverSocket.accept();
                    DataInputStream dis = new DataInputStream(sS.getInputStream());
                    String msg = dis.readUTF();
                    dis.close();
                    sS.close();
                    Log.v("serversocket",msg);
                    String[] strArr = msg.split(Pattern.quote("|"));
                    if(strArr.length > 2){
                        if(strArr[2].equals("INSERT")){
                            finalInsert(strArr[0],strArr[1]);
                        }
                        else if(strArr[2].equals("QUERY")){
                            int portNum = Integer.parseInt(strArr[1]);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNum);
                            String msgToSend = "";
                            if(strArr[0].equals("*")){
                                msgToSend = strArr[0]+"|"+queryAll(null)+"|"+"QUERYVALUE";
                            }
                            else {
                                msgToSend = strArr[0] + "|" + finalQuery(strArr[0]) + "|" + "QUERYVALUE";
                            }
                            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                            dos.writeUTF(msgToSend);
                            dos.flush();
                        }
                        else if(strArr[2].equals("QUERYVALUE")){
                            if(strArr[0].equals("*")){
                                if(strArr[1].isEmpty()){
                                    wait = false;
                                    continue;
                                }
                                String[] pairs = strArr[1].split(Pattern.quote("$"));
                                for(int i = 0; i <pairs.length; i++){
                                    String[] kv = pairs[i].split(Pattern.quote("&"));
                                    waitingQuery.put(kv[0],kv[1]);
                                }
                                wait = false;
                            }
                            else{
                                waitingQuery.put(strArr[0],strArr[1]);
                            }
                        }
                        else if(strArr[2].equals("DELETE")){
                            if(strArr[0].equals("*")){
                                deleteAll();
                            }
                            else{
                                getContext().deleteFile(strArr[0]);
                            }
                        }
                        else if(strArr[2].equals("PORTS")){
                            String[] ports = strArr[0].split(Pattern.quote("$"));
                            for(int i =0; i<ports.length; i++){
                                String[] kv = ports[i].split(Pattern.quote("&"));
                                Log.v("serversocket","before if ports "+kv[1]);
                                if(!allPortHash.containsKey(kv[0])){
                                    Log.v("serversocket","inside if ports "+kv[1]);
                                    allPortHash.put(kv[0],kv[1]);
                                }
                            }
                            Log.v("serversocket", "size of allPorthash " + allPortHash.size());
                        }
                    }
                    else {
                        if (allPortHash.containsKey(strArr[0])) {
                            Log.v("in allport condition", "already has the value");
                        }
                        else {
                            allPortHash.put(strArr[0], strArr[1]);
                            for(Map.Entry<String,String> me : allPortHash.entrySet()){
                                if(me.getValue().equals(localPort)){
                                    continue;
                                }
                                String updatedPorts = getAllPorts();
                                int portNum = Integer.parseInt(me.getValue());
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNum);
                                String msgToSend = updatedPorts + "|" + localPort+"|"+"PORTS";
                                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                                dos.writeUTF(msgToSend);
                                dos.flush();
                            }
                        }
                    }
                }
            } catch (IOException e) {
                Log.v("server","server Socket error in accept");
                e.printStackTrace();
            }
            //My code ends here
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
            Log.v("progress","Our received message is "+strReceived+" <-should be here");

            /*Code to save messages in content resolver or provider*/
            ContentValues cv = new ContentValues();
            cv.put("key",msgKey+"");
            msgKey++;
            cv.put("value",strReceived);
            mContentResolver.insert(mUri,cv);
            Log.v(TAG, "saving message went through");
            return;
        }
    }

    /*CLIENT Task is an AsyncTask to perform sending message */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            Log.v("msg to send",msgs[0]);
            Log.v("what port to send ",msgs[1]);
            try {
                String[] strArr = msgs[0].split(Pattern.quote("|"));
                if(strArr.length > 1){
                    int portNum = Integer.parseInt(msgs[1]);
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNum);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeUTF(msgs[0]);
                    dos.flush();
                }
                else{
                    int portNum = 11108;
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), portNum);
                    String msgToSend = msgs[0]+"|"+msgs[1];
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    dos.writeUTF(msgToSend);
                    dos.flush();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String getAllPorts(){
        String retval = "";
        int i = 0;
        for(Map.Entry<String,String> me : allPortHash.entrySet()){
            retval += me.getKey()+"&"+me.getValue();
            if(i<allPortHash.size()-1){
                retval+="$";
            }
            i++;
        }
        return retval;
    }
}
