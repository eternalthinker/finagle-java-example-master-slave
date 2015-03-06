/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.vizury.videocache.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author sankalpkulshrestha
 */
public class DBConnecter {

    Connection dbConnection = null;

    public DBConnecter(String host, String user, String password) {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            dbConnection = DriverManager.getConnection("jdbc:mysql://" + host + ":3306/vrm", user, password);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            Logger.getLogger(DBConnecter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private ResultSet execute(String query, List<Object> parameterList) {
        try {
            PreparedStatement preparedStatement = dbConnection.prepareStatement(query);
            if (parameterList != null) {
                int index = 1;
                for (Object parameter : parameterList) {
                    if (parameter.getClass() == Integer.class) {
                        preparedStatement.setInt(index, (int) parameter);
                    } else if (parameter.getClass() == String.class) {
                        preparedStatement.setString(index, (String) parameter);
                    } else if (parameter.getClass() == Float.class) {
                        preparedStatement.setFloat(index, (Float) parameter);
                    } else if (parameter.getClass() == Double.class) {
                        preparedStatement.setDouble(index, (Double) parameter);
                    }
                }
            }
            return preparedStatement.executeQuery();
        } catch (SQLException ex) {
            Logger.getLogger(DBConnecter.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public String getNamespace(HashMap<String, String> propertyMap, String campaignId) {
        List<Object> parameterList = new ArrayList<>();
        parameterList.add(Integer.parseInt(campaignId.substring(6)));
        String namespace = null;
        ResultSet resultSet = this.execute(propertyMap.get("memcachequery"), parameterList);
        if (resultSet != null) {
            try {
                while (resultSet.next()) {
                    namespace = resultSet.getString("namespace");
                    break;
                }
            } catch (SQLException ex) {
                Logger.getLogger(DBConnecter.class.getName()).log(Level.SEVERE, null, ex);
            }
            return namespace;
        } else {
            return null;
        }
    }

    public void disconnect() {
        try {
            dbConnection.close();
        } catch (SQLException ex) {
            Logger.getLogger(DBConnecter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
