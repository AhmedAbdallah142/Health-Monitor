package Bigdata.models;
public class Object {
    private int count ;
    private double ACpu;
    private double PCpu;
    private String TCpu;
    private double ADisk;
    private double PDisk;
    private String TDisk;
    private double ARam;
    private double PRam;
    private String TRam;
    public Object(){
        count=0;
        ACpu=0;  PCpu=0;   TCpu="";
        ADisk=0; PDisk=0;  TDisk="";
        ARam=0;  PRam=0;   TRam="";
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setACpu(double ACpu) {
        this.ACpu = ACpu;
    }

    public void setPCpu(double PCpu) {
        this.PCpu = PCpu;
    }

    public void setTCpu(String TCpu) {
        this.TCpu = TCpu;
    }

    public void setADisk(double ADisk) {
        this.ADisk = ADisk;
    }

    public void setPDisk(double PDisk) {
        this.PDisk = PDisk;
    }

    public void setTDisk(String TDisk) {
        this.TDisk = TDisk;
    }

    public void setARam(double ARam) {
        this.ARam = ARam;
    }

    public void setPRam(double PRam) {
        this.PRam = PRam;
    }

    public void setTRam(String TRam) {
        this.TRam = TRam;
    }

    public int getCount() {
        return count;
    }

    public double getACpu() {
        return ACpu;
    }

    public double getPCpu() {
        return PCpu;
    }

    public String getTCpu() {
        return TCpu;
    }

    public double getADisk() {
        return ADisk;
    }

    public double getPDisk() {
        return PDisk;
    }

    public String getTDisk() {
        return TDisk;
    }

    public double getARam() {
        return ARam;
    }

    public double getPRam() {
        return PRam;
    }

    public String getTRam() {
        return TRam;
    }

    @Override
    public String toString() {
        return  "count= " + count +
                ", ACpu= " + (ACpu/count) +
                ", PCpu= " + PCpu +
                ", TCpu= " + TCpu +
                ", ADisk= " + (ADisk/count) +
                ", PDisk= " + PDisk +
                ", TDisk= " + TDisk +
                ", ARam= " + (ARam/count) +
                ", PRam= " + PRam +
                ", TRam= " + TRam ;
    }
}

