package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 //  MBP111.0138.B16
 //  System Serial: C02P4SP9G3QH
 //  Created by Abhishek Gautam on 04/04/2016
 //  agautam2@buffalo.edu
 //  University at Buffalo, The State University of New York.
 //  Copyright © 2016 Gautam. All rights reserved.
 */

/*Understanding of SQLite for Android -  http://developer.android.com/training/basics/data-storage/databases.html */

public class DBHandler extends SQLiteOpenHelper
{
    private static  String dbName="mainTuple";
    private static int version=1;

    private static final String schema = "CREATE TABLE dhtRecords (key DATA, value DATA, association DATA)";

    public DBHandler(Context context)
    {
        super(context, dbName, null, version);
    }

    @Override
    public void onCreate(SQLiteDatabase db)
    {
        db.execSQL(schema);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int obsolete, int latest)
    {
        //logic understood from - http://stackoverflow.com/questions/3675032/drop-existing-table-in-sqlite-when-if-exists-operator-is-not-supported
        db.execSQL("DROP TABLE IF EXISTS dhtRecords");
        onCreate(db);
    }

}
