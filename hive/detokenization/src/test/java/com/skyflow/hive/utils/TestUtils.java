package com.skyflow.hive.utils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.mockito.Mockito;

public class TestUtils {
    public static void setupHiveContext() {
        // Mock HiveConf
        HiveConf mockConf = Mockito.mock(HiveConf.class);
        
        // Mock SessionState
        SessionState mockSession = Mockito.mock(SessionState.class);
        Mockito.when(mockSession.getConf()).thenReturn(mockConf);
        
        // Set mock session as current session
        SessionState.setCurrentSessionState(mockSession);
    }
} 