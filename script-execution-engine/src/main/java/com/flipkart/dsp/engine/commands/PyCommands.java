package com.flipkart.dsp.engine.commands;

public interface PyCommands {
    String SET_WD = "import os; os.chdir('%s')";
    String WRITE_TO_LOCAL_FILE = "%s.to_csv('%s', header=0, sep='%s', encoding='%s', quotechar='%s', index=False)";
    String SAVE_MODEL = "import pickle; pickle.dump(%s, open('%s', 'wb'))";
    String READ_MODEL = "import pickle; globals()['%s'] = pickle.load(open('%s', 'rb'))";
    String ASSIGN = "globals()['%s']=";
    String ASSIGN_HEADER = "%s.columns=[%s]";
    String IMPORT_DECIMAL = "from decimal import Decimal;globals()['%s']=";
    String IMPORT_DATETIME = "from datetime import datetime;globals()['%s']=";
    String DELETE_FOLDER="import shutil;shutil.rmtree('%s')";
}
