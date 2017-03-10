package happy.istudy.gdbt;

import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;
import ml.dmlc.xgboost4j.java.XGBoostError;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zqwu on 5/10/16.
 */
public class XgboostPredictModel {

    private Booster booster;

    public XgboostPredictModel(String modelPath,int nthread) {
        try {
            booster = XGBoost.loadModel(modelPath);
            booster.setParam("nthread",nthread);
        } catch (XGBoostError xgBoostError) {
            xgBoostError.printStackTrace();
        }
    }


    public XgboostPredictModel(String modelPath) {
        try {
            booster = XGBoost.loadModel(modelPath);
        } catch (XGBoostError xgBoostError) {
            xgBoostError.printStackTrace();
        }
    }

    public float[][] predict(float[] data,int nrow,int ncol){
        try {
            DMatrix testMat = new DMatrix(data,nrow,ncol);
            float[][] predicts = booster.predict(testMat);
            return predicts;
        } catch (XGBoostError xgBoostError) {
            xgBoostError.printStackTrace();
            return new float[1][1];
        }
    }

    public float[][] predict(DMatrix data){
        try {
            float[][] predicts = booster.predict(data);
            return predicts;
        } catch (XGBoostError xgBoostError) {
            xgBoostError.printStackTrace();
            return new float[1][1];
        }
    }


    public Map<String, Integer> getFeatureScore(String featureMap){
        try{
            return booster.getFeatureScore(featureMap);
        }catch (XGBoostError xgBoostError){
            xgBoostError.printStackTrace();
            return new HashMap<String,Integer>();
        }
    }

    public String[] getModelDump(String featureMap, boolean withStats){
        try{
            return booster.getModelDump(featureMap,withStats);
        }catch (XGBoostError xgBoostError){
            xgBoostError.printStackTrace();
            return new String[1];
        }
    }

//    public float[][] predict(String[] data){
//        try {
//            int length = data.length;
//            int[] indice = new int[length];
//            float[] values = new float[length];
//            Iterator<LabeledPoint> iter = LabeledPoint.fromSparseVector(0.0f,indice,values);
//            DMatrix dmtx = new DMatrix(iter,null);
//            float[][] predicts = booster.predict(dmtx);
//            return predicts;
//        } catch (XGBoostError xgBoostError) {
//            xgBoostError.printStackTrace();
//            return new float[1][1];
//        }
//    }
}
