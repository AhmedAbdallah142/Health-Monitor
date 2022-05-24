package Bigdata.Controller;

public class serviceModel {
    public String Name, Count, meanCPU, peakCPU, meanDisk, peakDisk, meanRAM, peakRAM;

    @Override
    public String toString() {
        return "serviceModel{" +
                "Name='" + Name + '\'' +
                ", Count='" + Count + '\'' +
                ", meanCPU='" + meanCPU + '\'' +
                ", peakCPU='" + peakCPU + '\'' +
                ", meanDisk='" + meanDisk + '\'' +
                ", peakDisk='" + peakDisk + '\'' +
                ", meanRAM='" + meanRAM + '\'' +
                ", peakRAM='" + peakRAM + '\'' +
                '}';
    }
}
