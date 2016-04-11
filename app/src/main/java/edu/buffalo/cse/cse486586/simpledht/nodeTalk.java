package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;
import android.database.Cursor;
import android.database.MatrixCursor;

/**
 //  MBP111.0138.B16
 //  System Serial: C02P4SP9G3QH
 //  Created by Abhishek Gautam on 04/04/2016
 //  agautam2@buffalo.edu
 //  University at Buffalo, The State University of New York.
 //  Copyright © 2016 Gautam. All rights reserved.
 */

public class nodeTalk implements Serializable
{
    private String language;
    private String key;
    private String value;
    private String selection;
    private int whoAmI;

    private HashMap<String,String> result;

    public nodeTalk(String language)
    {
        this.language=language;
    }
    public void setWhoAmI(int whoAmI)
    {
        this.whoAmI=whoAmI;
    }
    public void setKey(String key)
    {
        this.key=key;
    }
    public void setValue(String value)
    {
        this.value=value;
    }
    public void setSelection(String selection)
    {
        this.selection=selection;
    }
    public int getWhoAmI()
    {
        return whoAmI;
    }

    public String getLanguage()
    {
        return language;
    }
    public String getKey()
    {
        return key;
    }
    public String getValue()
    {
        return value;
    }
    public String getSelection()
    {
        return selection;
    }

}

