package Bigdata.monitor;

public class NewObject {
    private int C ;
    private double ACpu;
    private double PCpu;
    private double ADisk;
    private double PDisk;
    private double ARam;
    private double PRam;
    public NewObject(){
        C=0;
        ACpu=0;  PCpu=0;
        ADisk=0; PDisk=0;
        ARam=0;  PRam=0;
    }

    public void setCount(int count) {
        this.C = count;
    }

    public void setACpu(double ACpu) {
        this.ACpu = ACpu;
    }

    public void setPCpu(double PCpu) {
        this.PCpu = PCpu;
    }


    public void setADisk(double ADisk) {
        this.ADisk = ADisk;
    }

    public void setPDisk(double PDisk) {
        this.PDisk = PDisk;
    }

    public void setARam(double ARam) {
        this.ARam = ARam;
    }

    public void setPRam(double PRam) {
        this.PRam = PRam;
    }

  /*  public void setTRam(String TRam) {
        this.TRam = TRam;
    }*/

    public int getCount() {
        return C;
    }

    public double getACpu() {
        return ACpu;
    }

    public double getPCpu() {
        return PCpu;
    }

    public double getADisk() {
        return ADisk;
    }

    public double getPDisk() {
        return PDisk;
    }

    public double getARam() {
        return ARam;
    }

    public double getPRam() {
        return PRam;
    }

    @Override
    public String toString() {
        return  "count= " + C +
                ", ACpu= " + (ACpu/C) +
                ", PCpu= " + PCpu +
                ", ADisk= " + (ADisk/C) +
                ", PDisk= " + PDisk +
                ", ARam= " + (ARam/C) +
                ", PRam= " + PRam ;
    }
}
