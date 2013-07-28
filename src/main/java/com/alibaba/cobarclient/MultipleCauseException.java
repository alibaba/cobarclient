package com.alibaba.cobarclient;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: fujohnwang
 * Date: 7/28/13
 * Time: 2:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class MultipleCauseException extends Throwable {
    private List<Throwable> causes = new ArrayList<Throwable>();

    public MultipleCauseException(List<Throwable> causes) {
        if (!(causes == null || causes.isEmpty())) this.causes.addAll(causes);
    }

    public void add(Throwable cause) {
        this.causes.add(cause);
    }

    public List<Throwable> getCauses() {
        return new ArrayList<Throwable>(this.causes);
    }

}
