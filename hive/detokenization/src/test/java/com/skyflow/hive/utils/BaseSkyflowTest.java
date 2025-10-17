package com.skyflow.hive.utils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public abstract class BaseSkyflowTest {
    @Mock
    protected SessionState mockSessionState;
    
    @Mock
    protected HiveConf mockConf;

    @BeforeEach
    public void setUpBase() {
        MockitoAnnotations.openMocks(this);
        when(mockSessionState.getConf()).thenReturn(mockConf);
        SessionState.setCurrentSessionState(mockSessionState);
    }
}