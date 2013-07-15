package com.alibaba.cobar.client.router.rules.support;

import org.apache.commons.lang.Validate;

public class ModFunction implements IFunction2<Long, Long> {
    private Long modDenominator;
    
    public ModFunction(Long modDenominator)
    {
        Validate.notNull(modDenominator);
        this.modDenominator = modDenominator;
    }
    
    public Long apply(Long input) {
        Validate.notNull(input);
        
        Long result = input % this.modDenominator;
        return result;
    }

    public void setModDenominator(Long modDenominator) {
        Validate.notNull(modDenominator);
        this.modDenominator = modDenominator;
    }

    public Long getModDenominator() {
        return modDenominator;
    }

}
