package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
/**
 //  MBP111.0138.B16
 //  System Serial: C02P4SP9G3QH
 //  Created by Abhishek Gautam on 4/04/2016
 //  Template by Steve Ko, Assistant Professor, University at Buffalo, The State University of New York.
 //  agautam2@buffalo.edu
 //  University at Buffalo, The State University of New York.
 //  Copyright © 2016 Gautam. All rights reserved.
 */


public class SimpleDhtProvider extends ContentProvider {

    private int launchPort;
    private int designatedPort = 5554;
    static final int serverPort = 10000;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    ArrayList<Integer> nodeSpace = new ArrayList<Integer>();
    int soloFlag=0;
    String lDump = "@";
    String gDump = "*";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs)
    {
        // TODO Auto-generated method stub
        String key=selection;
        DBHandler dbHandler = new DBHandler(getContext());
        SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
        sqLiteDatabase.delete("dhtRecords", "key=?", new String[] {key});

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values)
    {
        String key = (String)values.get("key");
        String value = (String)values.get("value");
        int association=0;

        if(soloFlag==1)
        {
            // CASE - 1 ONE AVD EXISTS IN THE CHORD
            Log.v(TAG, "in case 1 of insert");
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();

            values.put("association", launchPort);
            sqLiteDatabase.insert("dhtRecords", null, values);
            sqLiteDatabase.close();
        }
        else if(soloFlag==0)
        {
            //CASE - 2 TWO OR MORE AVDs EXIST IN THE CHORD
            Log.v(TAG, "in case 2 of insert");
            if(launchPort==designatedPort)
            {
                // CASE - 2.1
                Log.v(TAG,"CASE 2.1");
                /*Sorting array list based on comparator - http://www.tutorialspoint.com/java/java_using_comparator.htm*/
                Collections.sort(nodeSpace, new Comparator<Integer>()
                {
                    @Override
                    public int compare(Integer here, Integer there) {
                        int compareDecision = 0;
                        try {
                            compareDecision = genHash(Integer.toString(here)).compareTo(genHash(Integer.toString(there)));
                        }
                        catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "got exception in comparator");
                        }
                        return compareDecision;
                    }
                });

                Iterator iterator = nodeSpace.iterator();
                while(iterator.hasNext())
                {

                    try
                    {
                        String string=(iterator.next()).toString();
                        if(genHash(key).compareTo(genHash(string)) <=0)
                        {
                            association=Integer.parseInt(string);
                            break;
                        }
                    }
                    catch (NoSuchAlgorithmException e)
                    {
                        Log.v(TAG,"Exception in key comparison");
                    }
                }
                if(association == 0)
                {
                    association = nodeSpace.get(0);
                }
                Log.v(TAG,"key is :"+key +"assoc is : "+association);

                if(association==designatedPort)
                {
                    //local insert
                    DBHandler dbHandler = new DBHandler(getContext());
                    SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();

                    values.put("association", association);
                    sqLiteDatabase.insert("dhtRecords", null, values);
                    sqLiteDatabase.close();
                }
                else
                {
                    //SENDING DATA TO OTHER NODE FOR INSERTION
                    
                    
                    nodeTalk communication = new nodeTalk("redirectedInsert");
                    communication.setKey(key);
                    communication.setValue(value);
                    communication.setWhoAmI(association);
                    Log.d(TAG, "insert: communication obj key is : "+ communication.getKey()+" and value is : "+communication.getValue() );
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), association * 2);
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        outputStream.writeObject(communication);
                    }
                    catch (IOException e)
                    {
                        Log.d(TAG, "insert: IO exception in insert");
                    }

                }

            }
            else if (launchPort!=designatedPort)
            {
                // CASE - 2.2
                Log.v(TAG, "CASE 2.2");
                nodeTalk communication = new nodeTalk("insert");
                communication.setKey(key);
                communication.setValue(value);
                communication.setWhoAmI(launchPort);
                Log.d(TAG, "insert: Launchport is : "+launchPort + "and desgnated is :"+designatedPort);

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, communication);
                Log.v(TAG,"Called client task");
            }
        }
        return uri;
    }

    @Override
    public boolean onCreate()
    {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        launchPort = Integer.parseInt(portStr);
        try
        {
            ServerSocket serverSocket = new ServerSocket(serverPort);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (IOException e)
        {
            e.getMessage();
            return false;
        }

        if(launchPort!=designatedPort)
        {
            Log.d(TAG, "not launched on 5554");
            nodeTalk communication = new nodeTalk("join");
            communication.setWhoAmI(launchPort);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, communication);
            Log.v(TAG,"node space count: " + nodeSpace.size());
        }
        else
        {
            Log.d(TAG, "i am 5554 and only one for now");
            soloFlag = 1;
        }

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder)
    {
        // TODO Auto-generated method stub

        Log.v(TAG,"inside Cursor query");
        Cursor cursor=null;
        String[] columns = {"key", "value"};
        MatrixCursor matrixCursor=new MatrixCursor(columns);


        if(selection.equals(lDump))
        {
            Log.v(TAG,"CASE LDUMP");
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            cursor = sqLiteDatabase.query(true, "dhtRecords", columns, null, null, null, null, null, null);
            cursor.moveToFirst();

        }
        else if(selection.equals(gDump))
        {
            //TODO
            Log.v(TAG,"CASE GDUMP");
            if(soloFlag==1)
            {
                Log.v(TAG, "When soloFlag is 1");
                DBHandler dbHandler = new DBHandler(getContext());
                SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
                cursor = sqLiteDatabase.query(true, "dhtRecords", columns, null, null, null, null, null, null);
            }
            else
            {   //WHEN SOLO FLAG IS NOT 1
                //TODO
                Log.v(TAG, "When soloFlag is NOT 1");
                if(launchPort==designatedPort)
                {
                    //* REQUEST on 5554
                    Log.v(TAG, "Request on 5554 for GDUMP");
                    int iterator;
                    DBHandler dbHandler = new DBHandler(getContext());
                    SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
                    String hugeString = "5554#";
                    Cursor c2=null;
                    c2 = sqLiteDatabase.query(true, "dhtRecords", columns, null, null, null, null, null, null);
                    //cursor.moveToFirst();
                            /* http://stackoverflow.com/questions/2810615/how-to-retrieve-data-from-cursor-class */
                    c2.moveToFirst();
                    Log.d(TAG, "query: Cursor count is : "+c2.getCount());
                    if(c2.getCount()>0)
                    {
                        do
                        {
                            hugeString+=c2.getString(0)+"-"+c2.getString(1)+"*";

                        }while (c2.moveToNext());
                    }
                    else
                    {
                        hugeString+="random-random*";
                    }
                    hugeString+="@";
                    for(int i=0;i<nodeSpace.size();i++)
                    {
                        iterator=nodeSpace.get(i);
                        if(iterator!=designatedPort)
                        {
                            //send message to nodeSpace.get(i)
                            try {
                                int myContactforLDUMP = nodeSpace.get(i);
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), myContactforLDUMP * 2);
                                nodeTalk communication = new nodeTalk("sendYourLDUMP");
                                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                                outputStream.writeObject(communication);

                                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                                String processMyContactforLDUMPReply = dataInputStream.readUTF();

                                hugeString+=processMyContactforLDUMPReply+"@";
                                Log.v(TAG,"@@huge string: "+ hugeString);


                            }
                            catch (IOException e)
                            {
                                e.printStackTrace();
                            }

                        }
                    }

                    MatrixCursor mc = new MatrixCursor(columns);

                    String[] splitter = hugeString.split("@");
                    int i=0;

                    while(i<splitter.length)
                    {
                        String[] splitter2 = splitter[i].split("#");
                        Log.v(TAG,"splitter2 at 0: " + splitter2[0] +"splitter2 at 1: "+ splitter2[1]);
                        String[] splitter3 = splitter2[1].split("\\*");
                        int j=0;
                        while(j<splitter3.length)
                        {
                            String[] splitter4 = splitter3[j].split("-");
                            if(!splitter4[0].equals("random")){
                            mc.addRow(new String[] {splitter4[0],splitter4[1]});}
                            j++;
                        }
                    i++;
                    }
                    try {
                        MergeCursor mergeCursor = new MergeCursor(new Cursor[]{mc, c2});
                        cursor = mergeCursor;
                        cursor.moveToFirst();
                    }
                    catch (NullPointerException e)
                    {
                        e.printStackTrace();
                    }

                }
                else if(launchPort!=designatedPort)
                {
                    Log.v(TAG, "REQUEST on OTHER DEVICES. 5554 takes care of it");
                    //* REQUEST on OTHER DEVICES. 5554 takes care of it
                    //TODO
                    try
                    {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), designatedPort * 2);
                        nodeTalk communication = new nodeTalk("redirectedGDUMP");
                        communication.setWhoAmI(launchPort);
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        outputStream.writeObject(communication);
                        Log.v(TAG,"redirectedGDUMP query object sent to ST of 5554");

                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String processDaemonReply = dataInputStream.readUTF();

                        if(processDaemonReply!=null)
                        {
                            Log.v(TAG,"Processing and splitting replies");
                            MatrixCursor mc = new MatrixCursor(columns);

                            String[] splitter = processDaemonReply.split("@");
                            int i=0;
                            while (i < splitter.length)
                            {
                                String[] splitter2 = splitter[i].split("#");
                                String[] splitter3 = splitter2[1].split("\\*");
                                int j=0;
                                while(j < splitter3.length)
                                {
                                    String[] splitter4 = splitter3[j].split("-");
                                    if(!splitter4[0].equals("random")){
                                    mc.addRow(new String[]{splitter4[0], splitter4[1]});}
                                    j++;
                                }
                                i++;
                            }
                            try
                            {
                            MergeCursor mergeCursor = new MergeCursor(new Cursor[]{mc, cursor});
                            cursor = mergeCursor;
                            cursor.moveToFirst();
                            }
                            catch (NullPointerException e)
                            {
                                e.printStackTrace();
                            }
                        }
                    }
                    catch(IOException e)
                    {
                        e.printStackTrace();
                    }

                }
            }
        }
        else
        {
            //Query case for specific key
            int association=0;
            String key = selection;
            if(launchPort==designatedPort)
            {
                //calculate association
                Log.v(TAG,"case where single node and 5554");
                Iterator iterator = nodeSpace.iterator();
                while(iterator.hasNext())
                {
                    try
                    {
                        String string=(iterator.next()).toString();
                        if(genHash(key).compareTo(genHash(string)) <=0)
                        {
                            association=Integer.parseInt(string);
                            break;
                        }
                    }
                    catch (NoSuchAlgorithmException e)
                    {
                        Log.v(TAG,"Exception in key comparison");
                    }
                }
                if(association == 0)
                {
                    association = nodeSpace.get(0);
                }
                Log.v(TAG,"association value is : "+association);
                if(association==designatedPort)
                {
                    //local query for specific key
                    Log.v(TAG,"local query for specific key");
                    DBHandler dbHandler = new DBHandler(getContext());
                    SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
                    cursor = sqLiteDatabase.query(true, "dhtRecords", columns, "key=?", new String[]{selection}, null, null, null, null);
                    Log.v(TAG,"after query is done");
                    //sqLiteDatabase.close();
                }
                else
                {
                    //redirected query for specific key
                    Log.v(TAG,"redirected query for specific key");

                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), association * 2);
                        nodeTalk communication = new nodeTalk("redirectedQuery");
                        communication.setKey(selection);
                        communication.setWhoAmI(association);
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        outputStream.writeObject(communication);
                        Log.v(TAG, "redirectedQuery sent to ST on socket "+socket.toString());

                        //BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String processDaemonReply = dataInputStream.readUTF();
                        Log.v(TAG,"after buffered reader");

                        if(processDaemonReply!=null)
                        {
                            Log.v(TAG,"daemon reply processed for redirected query as : " + processDaemonReply);
                            String[] tokenContainer = processDaemonReply.split("-");

                            try {
                                matrixCursor.addRow(new String[]{tokenContainer[1], tokenContainer[2]});
                                MergeCursor mergeCursor = new MergeCursor(new Cursor[]{matrixCursor, cursor});
                                cursor = mergeCursor;
                                cursor.moveToFirst();
                                Log.v(TAG, "processDaemonReply as: " + cursor.getString(0) +"-"+ cursor.getString(1));
                            }
                            catch (NullPointerException e)
                            {
                                e.printStackTrace();
                            }

                        }
                    }
                    catch(IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            else
            {
                //local query for specific key
                Cursor tempCursor=null;
                Log.v(TAG,"local query for specific key");
                DBHandler dbHandler = new DBHandler(getContext());
                SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
                tempCursor = sqLiteDatabase.query(true, "dhtRecords", columns, "key=?", new String[]{selection}, null, null, null, null);

                if(tempCursor.getCount()>0)
                {
                    cursor=tempCursor;
                }
                else
                {
                    //send to CT
                    nodeTalk communication = new nodeTalk("query");
                    communication.setWhoAmI(launchPort);
                    communication.setKey(selection);
                    AsyncTask<nodeTalk,Void,Cursor> queryTask= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, communication);
                    //see how to get output here
                    Log.v(TAG,"SPECIFIC QUERY not on 5554 and data not present sent to CT");
                    try {
                        if(queryTask != null)
                        {
                            cursor = queryTask.get();

                        }
                        else
                        {
                            Log.v(TAG, "asynctask returned null");
                        }
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                    catch (ExecutionException e)
                    {
                        e.printStackTrace();
                    }

                    //TODO close database

                }
            }

        }



        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void>
    {

        Socket socket = null;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.v(TAG, "%%%launch port is: " + launchPort);
            if(launchPort==designatedPort)
            {
                nodeSpace.add(designatedPort);
            }

            ServerSocket serverSocket = sockets[0];

            try {
                String daemonReply = "";
                while (true)
                {
                    socket = serverSocket.accept();
                    Log.v(TAG,"#SERVER" + launchPort + "LISTENING FOR INCOMING CONNECTIONS#");
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    nodeTalk communication = (nodeTalk)inputStream.readObject();
                    Log.v(TAG,"communication values are :"+communication.getKey()+" "+communication.getValue()+communication.getWhoAmI());
                    if (communication != null)
                    {
                        if (communication.getLanguage().equals("join"))
                        {
                            Log.v(TAG, " Language type is join");
                            daemonReply = joinHandler(communication);
                            Log.v(TAG, " Join Response to client sent");
                        }
                        if (communication.getLanguage().equals("insert"))
                        {
                            Log.v(TAG, " Language type is insert");
                            daemonReply = insertHandler(communication);
                            Log.v(TAG, " Insert Response to client sent");
                        }
                        if (communication.getLanguage().equals("redirectedInsert"))
                        {
                            Log.v(TAG, " Language type is redirectedInsert");
                            daemonReply = redirectedInsertHandler(communication);
                            Log.v(TAG, " redirectedInsert Response to client sent");
                        }
                        if (communication.getLanguage().equals("redirectedQuery"))
                        {
                            Log.v(TAG, " Language type is redirectedQuery");
                            daemonReply = redirectedQueryHandler(communication);
                            Log.v(TAG, " redirectedQuery Response to client sent: " + daemonReply);
                        }
                        if(communication.getLanguage().equals("query"))
                        {
                            Log.v(TAG, " Language type is Query");
                            daemonReply = queryHandler(communication);
                            Log.v(TAG, " Query Response to client sent: "+ daemonReply);
                        }
                        if(communication.getLanguage().equals("redirectedGDUMP"))
                        {
                            Log.v(TAG, " Language type is redirectedGDUMP");
                            daemonReply = redirectedGDUMPHandler(communication);
                            Log.v(TAG, " redirectedGDUMP Response to client sent");
                        }
                        if(communication.getLanguage().equals("sendYourLDUMP"))
                        {
                            Log.v(TAG, " Language type is sendYourLDUMP");
                            daemonReply = sendYourLDUMPHandler(communication);
                            Log.v(TAG, " sendYourLDUMP Response to client sent");
                        }

                        try
                        {
                            Log.v(TAG, "daemonReply sent from server on socket :" + socket.toString());
                            DataOutputStream dataOutPutStream = new DataOutputStream(socket.getOutputStream());
                            dataOutPutStream.writeUTF(daemonReply);
                        }
                        catch (IOException e)
                        {
                            e.printStackTrace();
                        }

                    }
                }//while true ends
            }
            catch(IOException e)
            {
                Log.v(TAG, e.getMessage());
            }
            catch(ClassNotFoundException e)
            {
                Log.v(TAG, e.getMessage());
            }
            return null;
        }

        String joinHandler(nodeTalk nodetalk)
        {
            nodeSpace.add(nodetalk.getWhoAmI());
            String daemonReply = "Welcome Aboard";
            soloFlag=0;
            return  daemonReply;
        }
        String insertHandler(nodeTalk nodetalk)
        {
            int association=0;
            ContentValues values=new ContentValues();
            String daemonReply="";
            {
                // CASE - 2.1
                Log.v(TAG,"inside insertHandler at Server");
                /*Sorting array list based on comparator - http://www.tutorialspoint.com/java/java_using_comparator.htm*/
                Collections.sort(nodeSpace, new Comparator<Integer>()
                {
                    @Override
                    public int compare(Integer here, Integer there) {
                        int compareDecision = 0;
                        try {
                            compareDecision = genHash(Integer.toString(here)).compareTo(genHash(Integer.toString(there)));
                        }
                        catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "got exception in comparator");
                        }
                        return compareDecision;
                    }
                });

                Iterator iterator = nodeSpace.iterator();
                while(iterator.hasNext())
                {
                    try
                    {
                        String string=(iterator.next()).toString();
                        if(genHash(nodetalk.getKey()).compareTo(genHash(string)) <=0)
                        {
                            association=Integer.parseInt(string);
                            break;
                        }
                    }
                    catch (NoSuchAlgorithmException e)
                    {
                        Log.v(TAG,"Exception in key comparison");
                    }
                }
                if(association == 0)
                {
                    association = nodeSpace.get(0);
                }
                Log.d(TAG, "insertHandler: key is "+nodetalk.getKey() +" and assoc is : "+association );
                if(association==designatedPort)
                {
                    //local insert
                    DBHandler dbHandler = new DBHandler(getContext());
                    SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();

                    values.put("key",nodetalk.getKey());
                    values.put("value",nodetalk.getValue());
                    values.put("association", association);
                    sqLiteDatabase.insert("dhtRecords", null, values);
                    sqLiteDatabase.close();
                    daemonReply="Insert successful";
                }
                else
                {
                    //SENDING DATA TO OTHER NODE FOR INSERTION
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), association * 2);
                        nodeTalk communication = new nodeTalk("redirectedInsert");
                        communication.setKey(nodetalk.getKey());
                        communication.setValue(nodetalk.getValue());
                        communication.setWhoAmI(association);
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        outputStream.writeObject(communication);

//                        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(socket.getInputStream()));
//                        String reply=bufferedReader.readLine();
                        daemonReply = "Insert success";

                    }
                    catch(IOException e)
                    {
                        e.printStackTrace();
                    }

                }

            }
            return daemonReply;
        } //insertHandler ends

        String redirectedInsertHandler(nodeTalk nodetalk)
        {
            String daemonReply="";
            Log.v(TAG,"communication values are :"+nodetalk.getKey()+" "+nodetalk.getValue()+nodetalk.getWhoAmI());

            ContentValues contentValues=new ContentValues();
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            contentValues.put("key", nodetalk.getKey());
            contentValues.put("value",nodetalk.getValue());
            contentValues.put("association", nodetalk.getWhoAmI());
            sqLiteDatabase.insert("dhtRecords", null, contentValues);
            sqLiteDatabase.close();
            daemonReply = "insert successful";
            return daemonReply;
        }

        String redirectedQueryHandler(nodeTalk nodetalk)
        {
            String daemonReply="";
            String key = nodetalk.getKey();
            Cursor cursor=null;
            String[] columns = {"key", "value"};

            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();

            cursor = sqLiteDatabase.query(true, "dhtRecords", columns, "key=?", new String[]{key}, null, null, null, null);
            cursor.moveToFirst();
            String value = cursor.getString(1);
            daemonReply = "singlequery-"+key+"-"+value;
            Log.d(TAG, "redirectedQueryHandler: reply from redirect query is :"+ daemonReply);
            return daemonReply;
        }
        String queryHandler(nodeTalk nodetalk)
        {
            //calculate association
            Log.v(TAG, "inside query handler at ST");
            int association=0;
            String key=nodetalk.getKey();
            Cursor cursor=null;
            String[] columns={"key", "value"};
            String daemonReply="";
            Iterator iterator = nodeSpace.iterator();
            while(iterator.hasNext())
            {
                try
                {
                    String string=(iterator.next()).toString();
                    if(genHash(key).compareTo(genHash(string)) <=0)
                    {
                        association=Integer.parseInt(string);
                        break;
                    }
                }
                catch (NoSuchAlgorithmException e)
                {
                    Log.v(TAG,"Exception in key comparison");
                }
            }
            if(association == 0)
            {
                association = nodeSpace.get(0);
            }

            if(association==designatedPort)
            {
                DBHandler dbHandler = new DBHandler(getContext());
                SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
                cursor = sqLiteDatabase.query(true, "dhtRecords", columns, "key=?", new String[]{key}, null, null, null, null);
                cursor.moveToFirst();
                String value = cursor.getString(1); //ag949t
                daemonReply = "singlequery-"+key+"-"+value;
                sqLiteDatabase.close();
            }
            else
            {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), association * 2);
                    nodeTalk communication = new nodeTalk("redirectedQuery");
                    communication.setKey(key);
                    communication.setWhoAmI(association);
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    outputStream.writeObject(communication);
                    Log.v(TAG, "redirectedQuery sent to ST");

                    //BufferedReader bufferedreader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    String response = dis.readUTF();
                    daemonReply=response;
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }

            return daemonReply;
        }
        String redirectedGDUMPHandler(nodeTalk nodetalk)
        {
            //TODO

            int iterator;
            Cursor cursor = null;
            String[] columns = {"key", "value"};

            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            String hugeString = "5554#";
            cursor = sqLiteDatabase.query(true, "dhtRecords", columns, null, null, null, null, null, null);
            //cursor.moveToFirst();
                            /* http://stackoverflow.com/questions/2810615/how-to-retrieve-data-from-cursor-class */
            cursor.moveToFirst();
            if(cursor.getCount()>0)
            {
                do
                {
                    hugeString+=cursor.getString(0)+"-"+cursor.getString(1)+"*";
                }while (cursor.moveToNext());
            }
            else
            {
                hugeString+="random-random*";
            }
            hugeString+="@";
            for(int i=0;i<nodeSpace.size();i++)
            {
                iterator=nodeSpace.get(i);
                if(iterator!=designatedPort)
                {
                    //send message to nodeSpace.get(i)
                    try {
                        int myContactforLDUMP = nodeSpace.get(i);
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), myContactforLDUMP * 2);
                        nodeTalk communication = new nodeTalk("sendYourLDUMP");
                        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                        outputStream.writeObject(communication);

                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String processMyContactforLDUMPReply = dataInputStream.readUTF();

                        hugeString+=processMyContactforLDUMPReply+"@";


                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }

                }
            }
            return hugeString;
        }
        String sendYourLDUMPHandler(nodeTalk nodetalk)
        {
            //TODO
            Cursor cursor = null;
            String[] columns = {"key","value"};
            DBHandler dbHandler = new DBHandler(getContext());
            SQLiteDatabase sqLiteDatabase = dbHandler.getWritableDatabase();
            cursor = sqLiteDatabase.query(true, "dhtRecords", columns, null, null, null, null, null, null);
            String hugeString=Integer.toString(launchPort)+ "#";
            cursor.moveToFirst();
            if(cursor.getCount()>0)
            {
                do
                {
                    hugeString+=cursor.getString(0)+"-"+cursor.getString(1)+"*";

                }while(cursor.moveToNext());
            }
            else
            {
                hugeString += "random-random*";
            }
            Log.v(TAG, "huge string sent from ST :" + hugeString);
            return  hugeString;
        }
    }

    private class ClientTask extends AsyncTask<nodeTalk, Void, Cursor>
    {
        @Override
        protected Cursor doInBackground(nodeTalk... params)
        {
            Log.d(TAG, "doInBackground: Inside do in background");
            String[] columns = {"key","value"};
            MatrixCursor matrixCursor = new MatrixCursor(columns);

            try {

                nodeTalk obj = params[0];
                String daemonMessage = "";
                Log.d(TAG, "doInBackground: object values are: "+ obj.getLanguage());
                if (obj.getLanguage().equals("join"))
                {
                    Log.v(TAG, "handling join at CT");
                    //daemonMessage = obj.getLanguage() + "#" + obj.getWhoAmI();
                }
                else if (obj.getLanguage().equals("insert"))
                {
                    Log.v(TAG, "handling insert at CT");
                }
                else if (obj.getLanguage().equals("query"))
                {
                    Log.v(TAG, "handling query at CT");
                }

                //other daemonMessages here

                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), designatedPort * 2);
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                Log.v(TAG, "about to write" + obj + "to stream");
                outputStream.writeObject(obj);
                Log.v(TAG, "object written with message: " + obj.getLanguage() + "and port" + obj.getWhoAmI());

                if (obj.getLanguage().equals("join"))
                {
                    Log.d(TAG, "doInBackground: inside reply of join");

                    try
                    {
                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String reply = dataInputStream.readUTF();
                        if (reply != null)
                        {
                           Log.v(TAG, "## JOIN reply received from server: " + reply);
                        }
                        else
                        {
                            Log.v(TAG, "nothing received from server");
                        }
                    }
                    catch(IOException e)
                    {
                        e.printStackTrace();
                    }


                }
                else if (obj.getLanguage().equals("insert"))
                {
                    Log.v(TAG,"Inside insert in CT");

                    try
                    {
                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String reply = dataInputStream.readUTF();
                        if (reply != null)
                        {
                            Log.v(TAG, "### INSERT reply received from server: " + reply);
                        }
                        else
                        {
                            Log.v(TAG, "nothing received from server");
                        }
                    }
                    catch(IOException e)
                    {
                        e.printStackTrace();
                    }

                }
              else  if (obj.getLanguage().equals("query")) {
                    try {
                        //BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String reply=dataInputStream.readUTF();
                        //String reply = bufferedReader.readLine();
                        if (reply != null) {   //process
                            String[] processedData = reply.split("-");
                            matrixCursor.addRow(new String[]{processedData[1], processedData[2]});
                            Log.v(TAG, "reply received from server for query: " + reply);
                        } else {

                            Log.v(TAG, "nothing received from server");
                        }
                    } catch (IOException e) {
                        Log.v(TAG, e.getMessage());
                    }

                }
               else if (obj.getLanguage().equals("redirectedInsert")) {
                        Log.v(TAG,"Inside redirected insert in CT");

                }
                socket.close();
            }
            catch(IOException e)
            {
                Log.v(TAG, "No join reply case");
                e.printStackTrace();
                soloFlag=1;
                designatedPort=launchPort;
                nodeSpace.add(launchPort);
            }
            return matrixCursor;
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

}
