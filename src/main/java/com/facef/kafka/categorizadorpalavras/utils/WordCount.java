package com.facef.kafka.categorizadorpalavras.utils;

import lombok.*;

import java.util.Date;

@AllArgsConstructor
@Getter
@Setter
@Builder
public class WordCount {

    private String key;
    private Long count;
    private Date start;
    private Date end;

}
